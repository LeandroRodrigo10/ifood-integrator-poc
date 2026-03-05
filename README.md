# iFood Integration Core (POC)

Event-driven marketplace integration prototype designed to demonstrate the architectural core of an ERP integration module.

This project simulates how an ERP can integrate with iFood using a resilient event-driven architecture.

## Key Concepts Demonstrated

• Polling ingestion (iFood)  
• Idempotent event processing  
• Redis Streams queue  
• Worker-based async processing (consumer group)  
• Controlled retry with backoff intent  
• Event audit logs  
• Manual event reprocessing  
• iFood ACK after processing (prevents duplicates)

## Architecture Overview

Polling → Event Persistence → Redis Streams → Worker → iFood API (details + ACK) → ERP Mock → Event Logs

Key architectural decisions:

• Polling endpoint performs no heavy processing  
• Events are persisted before queueing  
• Redis Streams ensures reliable delivery  
• Worker processes events asynchronously  
• Retry prevents event loss and increments attempts  
• All events are auditable and reprocessable  
• Idempotency key is UNIQUE in the database

## Lifecycle validated

This POC was validated with real iFood events and processed the full order lifecycle:

PLACED → CONFIRMED → DISPATCHED → CONCLUDED

Drop code events may appear and are audit-only.

## Endpoints

GET /health  
GET /events  
GET /events/:id/logs  
POST /events/:id/reprocess  
POST /ifood/poll  

## Running Locally

Install dependencies
```bash
npm install

Start Redis

docker compose up -d

Run migrations

npm run migrate

Start the HTTP server

npm run server

Start the worker

npm run worker
Testing

Trigger polling manually

curl -X POST http://localhost:3000/ifood/poll

List persisted events

curl http://localhost:3000/events

Get event logs

curl http://localhost:3000/events/:id/logs

Manual reprocess

curl -X POST http://localhost:3000/events/:id/reprocess
Notes

This project represents the architectural core of a marketplace integration module.

It is a proof of concept and uses an ERP mock to demonstrate flow, retry, auditing, and reprocessing.
