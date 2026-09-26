export type NodeId = "node-a" | "node-b" | "node-c"

export type KvOperation =
  | { type: "put"; key: string; value: string }
  | { type: "delete"; key: string }
  | { type: "noop" }

export type ClientOperation =
  | { type: "put"; key: string; value: string }
  | { type: "get"; key: string }
  | { type: "delete"; key: string }

export type ClientRequest = {
  id: string
  operation: ClientOperation
}

export type ClientResult = {
  id: string
  status: "ok" | "unavailable"
  value?: string | null
  version?: number
}

export type LogEntry = {
  index: number
  term: number
  operation: KvOperation
}

export type PersistedState = {
  term: number
  votedFor: NodeId | null
  log: LogEntry[]
  commitIndex: number
}

export type ProtocolMessage =
  | { type: "request_vote"; term: number; candidateId: NodeId; lastLogIndex: number; lastLogTerm: number }
  | { type: "vote_response"; term: number; voterId: NodeId; granted: boolean }
  | { type: "append_entries"; term: number; leaderId: NodeId; entries: LogEntry[]; leaderCommit: number; readId: string | null }
  | { type: "append_response"; term: number; followerId: NodeId; success: boolean; matchIndex: number; readId: string | null }

export type NodeInput =
  | { type: "start" }
  | { type: "client"; request: ClientRequest }
  | { type: "message"; from: NodeId; message: ProtocolMessage }
  | { type: "timer"; name: "election" | "heartbeat"; token: number }
  | { type: "storage"; id: string; value: string | null; error: string | null }

export type NodeEffect =
  | { type: "send"; to: NodeId; message: ProtocolMessage }
  | { type: "read"; id: string; key: string }
  | { type: "write"; id: string; key: string; value: string }
  | { type: "schedule"; name: "election" | "heartbeat"; token: number; delay: number }
  | { type: "client"; result: ClientResult }
  | { type: "trace"; action: string; data: Record<string, string | number | boolean | null>; target?: NodeId }

export type NodeRole = "follower" | "candidate" | "leader"
