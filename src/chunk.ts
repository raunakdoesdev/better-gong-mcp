import type { Monologue } from "./gong.js";

export type Chunk = { idx: number; start_s: number; end_s: number; text: string };

export const CHUNK_CHARS = 2000;

export const clock = (s: number) => {
  const t = Math.floor(s);
  const h = Math.floor(t / 3600);
  const mm = String(Math.floor((t % 3600) / 60)).padStart(2, "0");
  const ss = String(t % 60).padStart(2, "0");
  return h ? `${h}:${mm}:${ss}` : `${mm}:${ss}`;
};

type Line = { start_s: number; end_s: number; text: string };

/** Speaker turns as `[mm:ss] Name: text` lines, packed into chunks of about CHUNK_CHARS. */
export function chunkTranscript(monologues: readonly Monologue[], names: ReadonlyMap<string, string>, limit = CHUNK_CHARS): Chunk[] {
  const lines: Line[] = [];
  for (const m of monologues) {
    const who = (m.speakerId && names.get(m.speakerId)) || "Unknown speaker";
    const pieces: Line[] = [];
    let cur: Line | undefined;
    for (const s of m.sentences) {
      const text = s.text.trim();
      if (!text) continue;
      if (cur && cur.text.length + text.length + 1 <= limit) {
        cur.text += ` ${text}`;
        cur.end_s = s.end / 1000;
      } else {
        if (cur) pieces.push(cur);
        cur = { start_s: s.start / 1000, end_s: s.end / 1000, text };
      }
    }
    if (cur) pieces.push(cur);
    for (const p of pieces) lines.push({ ...p, text: `[${clock(p.start_s)}] ${who}: ${p.text}` });
  }
  const chunks: Chunk[] = [];
  let cur: Chunk | undefined;
  for (const line of lines) {
    if (cur && cur.text.length + line.text.length + 1 <= limit) {
      cur.text += `\n${line.text}`;
      cur.end_s = line.end_s;
    } else {
      if (cur) chunks.push(cur);
      cur = { idx: chunks.length, start_s: line.start_s, end_s: line.end_s, text: line.text };
    }
  }
  if (cur) chunks.push(cur);
  return chunks;
}
