import { applyLog, decodeState, encodeState } from "./model"
import type {
  ClientRequest,
  ClientResult,
  LogEntry,
  NodeEffect,
  NodeId,
  NodeInput,
  NodeRole,
  PersistedState,
  ProtocolMessage
} from "./types"

export type NodeConfig = {
  electionTimeout: number
  heartbeatInterval: number
}

type PendingAction =
  | { kind: "restore" }
  | { kind: "election"; state: PersistedState }
  | { kind: "vote"; state: PersistedState; candidateId: NodeId; granted: boolean }
  | { kind: "append"; state: PersistedState; from: NodeId; readId: string | null }
  | { kind: "client_append"; state: PersistedState; request: ClientRequest; entry: LogEntry }
  | { kind: "leadership_noop"; state: PersistedState }
  | { kind: "commit"; state: PersistedState; index: number }
  | { kind: "stepdown"; state: PersistedState }

type ActiveStorage = {
  id: string
  action: PendingAction
}

type PendingRead = {
  request: ClientRequest
  acknowledgements: Set<NodeId>
  minCommitIndex: number
}

const nodeIds: readonly NodeId[] = ["node-a", "node-b", "node-c"]
const stateKey = "state"

export class DatabaseNode {
  private state: PersistedState = { term: 0, votedFor: null, log: [], commitIndex: 0 }
  private role: NodeRole = "follower"
  private values = new Map<string, string>()
  private votes = new Set<NodeId>()
  private matchIndex = new Map<NodeId, number>()
  private pendingWrites = new Map<number, ClientRequest>()
  private pendingReads = new Map<string, PendingRead>()
  private activeStorage: ActiveStorage | null = null
  private queued: NodeInput[] = []
  private storageSequence = 0
  private readSequence = 0
  private electionToken = 0
  private heartbeatToken = 0
  private started = false
  private restored = false

  constructor(readonly id: NodeId, private readonly config: NodeConfig, private readonly incarnation = 0) {
    if (!Number.isSafeInteger(config.electionTimeout) || config.electionTimeout < 1) {
      throw new RangeError("Election timeout must be positive")
    }
    if (!Number.isSafeInteger(config.heartbeatInterval) || config.heartbeatInterval < 1) {
      throw new RangeError("Heartbeat interval must be positive")
    }
    if (config.heartbeatInterval * 2 >= config.electionTimeout) {
      throw new RangeError("Election timeout must exceed two heartbeat intervals")
    }
  }

  get currentRole(): NodeRole {
    return this.role
  }

  get isReady(): boolean {
    return this.restored
  }

  get currentTerm(): number {
    return this.state.term
  }

  get currentCommitIndex(): number {
    return this.state.commitIndex
  }

  get lastLogIndex(): number {
    return this.state.log.length
  }

  get committedValues(): ReadonlyMap<string, string> {
    return new Map(this.values)
  }

  get committedLog(): readonly LogEntry[] {
    return this.state.log.slice(0, this.state.commitIndex)
  }

  get logEntries(): readonly LogEntry[] {
    return this.state.log
  }

  handle(input: NodeInput): NodeEffect[] {
    if (input.type === "storage") return this.completeStorage(input)
    if (this.activeStorage) {
      this.queued.push(input)
      return []
    }

    const effects = this.process(input)
    return this.drainQueued(effects)
  }

  private process(input: NodeInput): NodeEffect[] {
    if (input.type === "start") return this.start()
    if (input.type === "client") return this.clientRequest(input.request)
    if (input.type === "message") return this.receive(input.from, input.message)
    if (input.type === "timer") return this.onTimer(input.name, input.token)
    return []
  }

  private start(): NodeEffect[] {
    if (this.started) return []
    this.started = true
    const id = this.nextStorageId()
    this.activeStorage = { id, action: { kind: "restore" } }
    return [{ type: "read", id, key: stateKey }]
  }

  private completeStorage(input: Extract<NodeInput, { type: "storage" }>): NodeEffect[] {
    if (!this.activeStorage || this.activeStorage.id !== input.id) return []
    const pending = this.activeStorage
    this.activeStorage = null
    let effects: NodeEffect[] = []

    if (pending.action.kind === "restore") {
      if (input.error) throw new Error(`Could not restore ${this.id}: ${input.error}`)
      this.installState(decodeState(input.value))
      this.restored = true
      effects.push(this.effectTrace("RESTORE", { term: this.state.term, index: this.state.commitIndex }))
      effects.push(...this.resetElectionTimer())
    } else if (input.error) {
      effects.push(...this.storageFailed(pending.action))
    } else {
      effects.push(...this.storageSucceeded(pending.action))
    }

    return this.drainQueued(effects)
  }

  private storageSucceeded(action: Exclude<PendingAction, { kind: "restore" }>): NodeEffect[] {
    this.installState(action.state)

    if (action.kind === "election") {
      this.role = "candidate"
      this.votes = new Set([this.id])
      const last = this.state.log[this.state.log.length - 1]
      const message: ProtocolMessage = {
        type: "request_vote",
        term: this.state.term,
        candidateId: this.id,
        lastLogIndex: this.state.log.length,
        lastLogTerm: last?.term ?? 0
      }
      const effects = this.peers().map(to => ({ type: "send", to, message }) as NodeEffect)
      effects.push(this.effectTrace("ELECTION_START", { term: this.state.term }))
      effects.push(...this.resetElectionTimer())
      return effects
    }

    if (action.kind === "vote") {
      const effects: NodeEffect[] = [
        { type: "send", to: action.candidateId, message: { type: "vote_response", term: this.state.term, voterId: this.id, granted: action.granted } },
        this.effectTrace("VOTE", { term: this.state.term, granted: action.granted }, action.candidateId)
      ]
      if (action.granted) effects.push(...this.resetElectionTimer())
      return effects
    }

    if (action.kind === "append") {
      const effects: NodeEffect[] = [
        { type: "send", to: action.from, message: { type: "append_response", term: this.state.term, followerId: this.id, success: true, matchIndex: this.state.log.length, readId: action.readId } },
        this.effectTrace("APPEND_STORED", { index: this.state.log.length }, action.from)
      ]
      effects.push(...this.resetElectionTimer())
      return effects
    }

    if (action.kind === "client_append") {
      this.matchIndex.set(this.id, this.state.log.length)
      this.pendingWrites.set(action.entry.index, action.request)
      const effects = this.broadcast()
      effects.push(...this.maybeCommit())
      return effects
    }

    if (action.kind === "leadership_noop") {
      this.matchIndex.set(this.id, this.state.log.length)
      return [...this.broadcast(), ...this.maybeCommit()]
    }

    if (action.kind === "commit") {
      this.values = applyLog(this.values, this.state.log, action.index)
      const effects: NodeEffect[] = []
      for (const [index, request] of this.pendingWrites) {
        if (index <= action.index) {
          effects.push({ type: "client", result: { id: request.id, status: "ok", version: index } })
          effects.push(this.effectTrace("CLIENT_ACK_QUORUM", { index }))
          this.pendingWrites.delete(index)
        }
      }
      effects.push(this.effectTrace("COMMIT", { index: action.index }))
      effects.push(...this.broadcast())
      return effects
    }

    if (action.kind === "stepdown") {
      this.role = "follower"
      this.votes.clear()
      return this.resetElectionTimer()
    }

    return []
  }

  private storageFailed(action: Exclude<PendingAction, { kind: "restore" }>): NodeEffect[] {
    if (action.kind === "append") {
      return [{
        type: "send",
        to: action.from,
        message: { type: "append_response", term: this.state.term, followerId: this.id, success: false, matchIndex: this.state.log.length, readId: action.readId }
      }]
    }
    if (action.kind === "client_append") {
      return [{ type: "client", result: { id: action.request.id, status: "unavailable" } }]
    }
    if (action.kind === "election") {
      return this.resetElectionTimer()
    }
    if (action.kind === "vote") {
      return []
    }
    if (action.kind === "commit") {
      return []
    }
    if (action.kind === "leadership_noop") {
      this.role = "follower"
      return this.resetElectionTimer()
    }
    this.role = "follower"
    return this.resetElectionTimer()
  }

  private drainQueued(effects: NodeEffect[]): NodeEffect[] {
    while (!this.activeStorage && this.queued.length > 0) {
      const next = this.queued.shift()!
      effects.push(...this.process(next))
    }
    return effects
  }

  private receive(from: NodeId, message: ProtocolMessage): NodeEffect[] {
    if (message.type === "request_vote") return this.requestVote(from, message)
    if (message.type === "vote_response") return this.voteResponse(message)
    if (message.type === "append_entries") return this.appendEntries(from, message)
    return this.appendResponse(message)
  }

  private requestVote(from: NodeId, message: Extract<ProtocolMessage, { type: "request_vote" }>): NodeEffect[] {
    if (message.term < this.state.term) {
      return [{ type: "send", to: from, message: { type: "vote_response", term: this.state.term, voterId: this.id, granted: false } }]
    }

    const last = this.state.log[this.state.log.length - 1]
    const upToDate = message.lastLogTerm > (last?.term ?? 0) ||
      (message.lastLogTerm === (last?.term ?? 0) && message.lastLogIndex >= this.state.log.length)
    const termChanged = message.term > this.state.term
    const previousVote = termChanged ? null : this.state.votedFor
    const canVote = previousVote === null || previousVote === message.candidateId
    const granted = upToDate && canVote
    const nextState = {
      ...this.state,
      term: message.term,
      votedFor: granted ? message.candidateId : previousVote
    }

    const steppedDown = termChanged ? this.stepDown() : []
    if (!termChanged && (!granted || this.state.votedFor === message.candidateId)) {
      return [...steppedDown, { type: "send", to: from, message: { type: "vote_response", term: this.state.term, voterId: this.id, granted } }]
    }

    return [...steppedDown, ...this.persist(nextState, { kind: "vote", state: nextState, candidateId: from, granted })]
  }

  private voteResponse(message: Extract<ProtocolMessage, { type: "vote_response" }>): NodeEffect[] {
    if (message.term > this.state.term) {
      const effects = this.stepDown()
      const nextState = { ...this.state, term: message.term, votedFor: null }
      return [...effects, ...this.persist(nextState, { kind: "stepdown", state: nextState })]
    }
    if (this.role !== "candidate" || message.term !== this.state.term || !message.granted) return []
    this.votes.add(message.voterId)
    if (this.votes.size < 2) return []
    this.role = "leader"
    this.matchIndex = new Map([[this.id, this.state.log.length]])
    this.matchIndex.set(this.peers()[0], 0)
    this.matchIndex.set(this.peers()[1], 0)
    this.heartbeatToken += 1
    const effects: NodeEffect[] = [this.effectTrace("BECOME_LEADER", { term: this.state.term })]
    effects.push({ type: "schedule", name: "heartbeat", token: this.heartbeatToken, delay: this.config.heartbeatInterval })
    const entry: LogEntry = { index: this.state.log.length + 1, term: this.state.term, operation: { type: "noop" } }
    const nextState = { ...this.state, log: [...this.state.log, entry] }
    effects.push(...this.persist(nextState, { kind: "leadership_noop", state: nextState }))
    return effects
  }

  private appendEntries(from: NodeId, message: Extract<ProtocolMessage, { type: "append_entries" }>): NodeEffect[] {
    if (message.term < this.state.term) {
      return [{
        type: "send",
        to: from,
        message: { type: "append_response", term: this.state.term, followerId: this.id, success: false, matchIndex: this.state.log.length, readId: message.readId }
      }]
    }

    const termChanged = message.term > this.state.term
    const steppedDown = termChanged || this.role !== "follower" ? this.stepDown() : []
    const nextTerm = Math.max(this.state.term, message.term)
    const incoming = message.entries.map(entry => ({ ...entry, operation: { ...entry.operation } }))
    const conflict = this.committedConflict(incoming)
    if (conflict) {
      return [...steppedDown, {
        type: "send",
        to: from,
        message: { type: "append_response", term: this.state.term, followerId: this.id, success: false, matchIndex: this.state.log.length, readId: message.readId }
      }]
    }

    const nextLog = this.mergeLog(incoming)
    const nextCommit = Math.max(this.state.commitIndex, Math.min(message.leaderCommit, nextLog.length))
    const nextState: PersistedState = {
      term: nextTerm,
      votedFor: termChanged ? null : this.state.votedFor,
      log: nextLog,
      commitIndex: nextCommit
    }
    const changed = JSON.stringify(nextState) !== JSON.stringify(this.state)
    if (!changed) {
      const effects: NodeEffect[] = [{
        type: "send",
        to: from,
        message: { type: "append_response", term: this.state.term, followerId: this.id, success: true, matchIndex: this.state.log.length, readId: message.readId }
      }]
      effects.push(...steppedDown)
      effects.push(...this.resetElectionTimer())
      return effects
    }

    return [...steppedDown, ...this.persist(nextState, { kind: "append", state: nextState, from, readId: message.readId })]
  }

  private appendResponse(message: Extract<ProtocolMessage, { type: "append_response" }>): NodeEffect[] {
    if (message.term > this.state.term) {
      const effects = this.stepDown()
      const nextState = { ...this.state, term: message.term, votedFor: null }
      return [...effects, ...this.persist(nextState, { kind: "stepdown", state: nextState })]
    }
    if (this.role !== "leader" || message.term !== this.state.term || !message.success) return []

    this.matchIndex.set(message.followerId, Math.max(this.matchIndex.get(message.followerId) ?? 0, message.matchIndex))
    const effects: NodeEffect[] = []
    if (message.readId) {
      const pending = this.pendingReads.get(message.readId)
      if (pending && message.matchIndex >= pending.minCommitIndex) {
        pending.acknowledgements.add(message.followerId)
        if (pending.acknowledgements.size >= 2) {
          effects.push(this.finishRead(message.readId, pending))
          this.pendingReads.delete(message.readId)
        }
      }
    }
    effects.push(...this.maybeCommit())
    return effects
  }

  private clientRequest(request: ClientRequest): NodeEffect[] {
    if (this.role !== "leader") return [{ type: "client", result: { id: request.id, status: "unavailable" } }]

    if (request.operation.type === "get") {
      const readId = `${this.id}:read:${this.readSequence}`
      this.readSequence += 1
      const pending: PendingRead = {
        request,
        acknowledgements: new Set([this.id]),
        minCommitIndex: this.state.commitIndex
      }
      this.pendingReads.set(readId, pending)
      return [...this.broadcast(readId), ...this.finishQuorumReadIfReady(readId, pending)]
    }

    const operation = request.operation.type === "put"
      ? { type: "put" as const, key: request.operation.key, value: request.operation.value }
      : { type: "delete" as const, key: request.operation.key }
    const entry: LogEntry = { index: this.state.log.length + 1, term: this.state.term, operation }
    const nextState = { ...this.state, log: [...this.state.log, entry] }
    return this.persist(nextState, { kind: "client_append", state: nextState, request, entry })
  }

  private onTimer(name: "election" | "heartbeat", token: number): NodeEffect[] {
    if (name === "election") {
      if (token !== this.electionToken || this.role === "leader") return []
      const term = this.state.term + 1
      const nextState = { ...this.state, term, votedFor: this.id }
      const effects = this.resetElectionTimer()
      effects.push(...this.persist(nextState, { kind: "election", state: nextState }))
      return effects
    }

    if (token !== this.heartbeatToken || this.role !== "leader") return []
    this.heartbeatToken += 1
    const effects: NodeEffect[] = [{ type: "schedule", name: "heartbeat", token: this.heartbeatToken, delay: this.config.heartbeatInterval }]
    effects.push(...this.broadcast())
    return effects
  }

  private maybeCommit(): NodeEffect[] {
    if (this.role !== "leader" || this.activeStorage) return []
    for (let index = this.state.log.length; index > this.state.commitIndex; index -= 1) {
      if (this.state.log[index - 1].term !== this.state.term) continue
      let copies = 1
      for (const peer of this.peers()) if ((this.matchIndex.get(peer) ?? 0) >= index) copies += 1
      if (copies >= 2) {
        const nextState = { ...this.state, commitIndex: index }
        return this.persist(nextState, { kind: "commit", state: nextState, index })
      }
    }
    return []
  }

  private broadcast(readId: string | null = null): NodeEffect[] {
    if (this.role !== "leader") return []
    return this.peers().map(to => ({
      type: "send",
      to,
      message: {
        type: "append_entries",
        term: this.state.term,
        leaderId: this.id,
        entries: this.state.log,
        leaderCommit: this.state.commitIndex,
        readId
      }
    }))
  }

  private mergeLog(incoming: LogEntry[]): LogEntry[] {
    const shared = Math.min(incoming.length, this.state.log.length)
    let common = 0
    while (common < shared && this.sameEntry(incoming[common], this.state.log[common])) common += 1
    if (common === incoming.length) return this.state.log
    return [...this.state.log.slice(0, common), ...incoming.slice(common)]
  }

  private committedConflict(incoming: readonly LogEntry[]): boolean {
    if (incoming.length < this.state.commitIndex) return true
    const shared = Math.min(incoming.length, this.state.log.length, this.state.commitIndex)
    for (let index = 0; index < shared; index += 1) {
      if (!this.sameEntry(incoming[index], this.state.log[index])) return true
    }
    return false
  }

  private sameEntry(left: LogEntry, right: LogEntry): boolean {
    return left.index === right.index && left.term === right.term && JSON.stringify(left.operation) === JSON.stringify(right.operation)
  }

  private finishQuorumReadIfReady(readId: string, pending: PendingRead): NodeEffect[] {
    if (pending.acknowledgements.size < 2) return []
    this.pendingReads.delete(readId)
    return [this.finishRead(readId, pending)]
  }

  private finishRead(readId: string, pending: PendingRead): NodeEffect {
    const key = pending.request.operation.type === "get" ? pending.request.operation.key : ""
    return {
      type: "client",
      result: {
        id: pending.request.id,
        status: "ok",
        value: this.values.get(key) ?? null,
        version: this.state.commitIndex
      }
    }
  }

  private persist(state: PersistedState, action: Exclude<PendingAction, { kind: "restore" }>): NodeEffect[] {
    if (this.activeStorage) throw new Error(`${this.id} attempted concurrent storage writes`)
    const id = this.nextStorageId()
    this.activeStorage = { id, action }
    return [{ type: "write", id, key: stateKey, value: encodeState(state) }]
  }

  private installState(state: PersistedState): void {
    this.state = state
    this.values = applyLog(new Map(), state.log, state.commitIndex)
  }

  private resetElectionTimer(): NodeEffect[] {
    this.electionToken += 1
    return [{ type: "schedule", name: "election", token: this.electionToken, delay: this.config.electionTimeout }]
  }

  private stepDown(): NodeEffect[] {
    this.role = "follower"
    this.votes.clear()
    this.matchIndex.clear()
    const effects: NodeEffect[] = []
    for (const request of this.pendingWrites.values()) {
      effects.push({ type: "client", result: { id: request.id, status: "unavailable" } })
    }
    for (const pending of this.pendingReads.values()) {
      effects.push({ type: "client", result: { id: pending.request.id, status: "unavailable" } })
    }
    this.pendingWrites.clear()
    this.pendingReads.clear()
    return effects
  }

  private peers(): NodeId[] {
    return nodeIds.filter(node => node !== this.id)
  }

  private nextStorageId(): string {
    const id = `${this.id}:${this.incarnation}:storage:${this.storageSequence}`
    this.storageSequence += 1
    return id
  }

  private effectTrace(action: string, data: Record<string, string | number | boolean | null>, target?: NodeId): NodeEffect {
    return { type: "trace", action, data, target }
  }
}
