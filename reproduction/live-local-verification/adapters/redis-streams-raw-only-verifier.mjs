#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { createClient } from "redis";

const [fixtureRoot, bundleRoot, redisUrl, runId] = process.argv.slice(2);
if (![fixtureRoot, bundleRoot, redisUrl, runId].every(Boolean)) throw new Error("usage: redis-streams-raw-only-verifier.mjs <fixtureRoot> <bundleRoot> <redisUrl> <runId>");
const stream = (suffix) => `flowplane:redis:${runId.toLowerCase()}:${suffix}`;
const streams = { raw: stream("raw"), transformed: stream("transformed"), dlq: stream("dlq") };
const readText = (file) => fs.readFileSync(file, "utf8").replace(/^\uFEFF/, ""); const readJsonl = (file) => readText(file).split(/\r?\n/).filter(Boolean).map(JSON.parse);
const valid = readJsonl(path.join(fixtureRoot, "valid-input.jsonl")); const invalid = readJsonl(path.join(fixtureRoot, "invalid-input.jsonl"));
const simulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-batch.json"))); const invalidSimulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-invalid.json")));
const expectedById = new Map(simulation.records.map((record) => [record.recordId, record.outputPreview]));
const expectedErrorCodes = [...new Set(invalidSimulation.errors.map((error) => { const match=String(error).match(/^[^:]+:\s+([A-Z0-9_]+)\s+-/); if(!match) throw new Error(`Cannot extract expected error code: ${error}`); return match[1]; }))].sort();
const sortJson=(value)=>Array.isArray(value)?value.map(sortJson):value&&typeof value==="object"?Object.fromEntries(Object.keys(value).sort().map((key)=>[key,sortJson(value[key])])):value; const canonical=(value)=>JSON.stringify(sortJson(value)); const sha256=(value)=>crypto.createHash("sha256").update(value,"utf8").digest("hex");
const redis=createClient({url:redisUrl}); await redis.connect();
const outputGroup=`verify-output-${runId.toLowerCase()}`; const dlqGroup=`verify-dlq-${runId.toLowerCase()}`;
for (const [key, group] of [[streams.transformed,outputGroup],[streams.dlq,dlqGroup]]) { try { await redis.xGroupCreate(key,group,"0",{MKSTREAM:true}); } catch(error) { if(!String(error).includes("BUSYGROUP")) throw error; } }
const rawInput=[...valid.map((payload)=>({payload,recordId:payload.event.id,kind:"valid",runId,publishedBy:"raw-only-verifier"})),...invalid.map((payload)=>({payload,recordId:payload.recordId,kind:"invalid",runId,publishedBy:"raw-only-verifier"}))]; const started=Date.now();
// The verifier's only XADD destination is the raw stream.
for (const record of rawInput) await redis.xAdd(streams.raw,"*",{data:JSON.stringify(record)});
const receive=async(key,group,count)=>{const rows=[];for(let i=0;i<count;i+=1){const batches=await redis.xReadGroup(group,`verifier-${runId}`, [{key,id:">"}],{COUNT:1,BLOCK:90_000});const message=batches?.[0]?.messages?.[0];if(!message)throw new Error(`Redis verifier timed out ${i}/${count}`);rows.push(JSON.parse(message.message.data));await redis.xAck(key,group,message.id);}return rows;};
const actualSuccess=await receive(streams.transformed,outputGroup,valid.length); const actualErrors=await receive(streams.dlq,dlqGroup,invalid.length);
const seenSuccess=new Set();let duplicates=0;let expectedHashMatches=0;const outputHashRows=[];for(const record of actualSuccess){if(seenSuccess.has(record.recordId))duplicates+=1;seenSuccess.add(record.recordId);const actualHash=sha256(canonical(record.payload));const expectedHash=sha256(canonical(expectedById.get(record.recordId)));if(actualHash===expectedHash)expectedHashMatches+=1;outputHashRows.push({recordId:record.recordId,expectedHash,actualHash,matched:actualHash===expectedHash});}
const seenErrors=new Set();let expectedErrorMatches=0;for(const record of actualErrors){if(seenErrors.has(record.recordId))duplicates+=1;seenErrors.add(record.recordId);const codes=[...new Set((record.payload?.errors??[]).map((error)=>error.code))].sort();if(canonical(codes)===canonical(expectedErrorCodes))expectedErrorMatches+=1;}
fs.writeFileSync(path.join(bundleRoot,"actual","transformed-output.jsonl"),`${actualSuccess.map(JSON.stringify).join("\n")}\n`);fs.writeFileSync(path.join(bundleRoot,"actual","error-output.jsonl"),`${actualErrors.map(JSON.stringify).join("\n")}\n`);fs.writeFileSync(path.join(bundleRoot,"actual","output-hashes.json"),`${JSON.stringify(outputHashRows,null,2)}\n`);
const report={schemaVersion:"flowplane.redis-streams-raw-only-verification-result.v1",runId,streams,boundary:"raw-only verifier -> Redis raw stream -> independent consumer-group pipeline -> Flowplane sidecar -> Redis output/DLQ -> read-only verifier groups",verifierWriteTargets:[streams.raw],verifierReadTargets:[streams.transformed,streams.dlq],attemptedInput:rawInput.length,acceptedInput:rawInput.length,validInput:valid.length,intentionalInvalid:invalid.length,successfulOutput:actualSuccess.length,errorOutput:actualErrors.length,filtered:0,duplicates,expectedHashMatches,expectedErrorMatches,expectedErrorCodes,durationSeconds:Math.round((Date.now()-started)/10)/100};fs.writeFileSync(path.join(bundleRoot,"actual","bridge-result.json"),`${JSON.stringify(report,null,2)}\n`);console.log(JSON.stringify(report));await redis.quit();
