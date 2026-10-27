import { describe, expect, it } from "vitest";
import { chunkTranscript, clock } from "../src/chunk.js";
import { accountOf } from "../src/ingest.js";
import { ftsQuery, readOnlySql } from "../src/search.js";

const names = new Map([["s1", "Ana"], ["s2", "Bo"]]);
const sentence = (start: number, text: string) => ({ start: start * 1000, end: start * 1000 + 900, text });

describe("chunkTranscript", () => {
  it("labels each turn with its time and speaker", () => {
    const chunks = chunkTranscript(
      [
        { speakerId: "s1", sentences: [sentence(0, "Hi."), sentence(1, "How are you?")] },
        { speakerId: "s2", sentences: [sentence(65, "Good.")] },
        { speakerId: "zz", sentences: [sentence(3700, "Bye.")] },
      ],
      names,
    );
    expect(chunks).toHaveLength(1);
    expect(chunks[0]!.text).toBe("[00:00] Ana: Hi. How are you?\n[01:05] Bo: Good.\n[1:01:40] Unknown speaker: Bye.");
    expect(chunks[0]!.end_s).toBeCloseTo(3700.9);
  });

  it("splits long turns and chunks without losing words, and every piece keeps its speaker", () => {
    const words = Array.from({ length: 60 }, (_, i) => sentence(i * 10, `sentence number ${i}.`));
    const chunks = chunkTranscript([{ speakerId: "s1", sentences: words }], names, 100);
    expect(chunks.length).toBeGreaterThan(5);
    expect(chunks.map((c) => c.idx)).toEqual(chunks.map((_, i) => i));
    for (const c of chunks) {
      expect(c.text.length).toBeLessThanOrEqual(120);
      for (const line of c.text.split("\n")) expect(line).toMatch(/^\[\d\d:\d\d\] Ana: /);
    }
    const all = chunks.map((c) => c.text.replace(/\[[\d:]+\] Ana: /g, "")).join(" ");
    for (let i = 0; i < 60; i++) expect(all).toContain(`sentence number ${i}.`);
  });
});

describe("helpers", () => {
  it("formats clocks", () => {
    expect(clock(59.9)).toBe("00:59");
    expect(clock(3600)).toBe("1:00:00");
  });

  it("quotes FTS terms", () => {
    expect(ftsQuery(`pricing "per-page" AND (OCR)*`)).toBe(`"pricing" OR "per-page" OR "AND" OR "OCR"`);
    expect(ftsQuery("  ?? ")).toBe("");
  });

  it("allows only single read statements", () => {
    expect(readOnlySql("SELECT count(*) FROM calls")).toBe(true);
    expect(readOnlySql("with x as (select 1) select * from x;")).toBe(true);
    expect(readOnlySql("DELETE FROM calls")).toBe(false);
    expect(readOnlySql("select 1; drop table calls")).toBe(false);
    expect(readOnlySql("WITH x AS (SELECT 1) DELETE FROM calls")).toBe(false);
  });

  it("reads the Salesforce account from call context", () => {
    const account = accountOf({
      metaData: { id: "1" },
      context: [{ system: "Salesforce", objects: [
        { objectType: "Opportunity", objectId: "o1", fields: [] },
        { objectType: "Account", objectId: "a1", fields: [{ name: "Name", value: "Acme" }, { name: "Industry", value: "Insurance" }] },
      ] }],
    });
    expect(account).toEqual({ id: "a1", name: "Acme", fields: { Name: "Acme", Industry: "Insurance" } });
  });
});
