//
// KVStore.swift
//
// Created by Gardner von Holt on 9/29/25.
//
// See TxnLogger.swift for durable transaction logging.
//

import Foundation
import GRDB
import Logging

actor KVStore {
    private let dbQueue: DatabaseQueue
    private let logger: TransactionLogging
    private static let oslog = Logger(label: "KVStore")

    init(path: String, logger: TransactionLogging? = nil) throws {
        Self.oslog.info("Opening database", metadata: ["path": .string(path)])
        // NOTE: Using DatabaseQueue for simplicity; can be swapped to DatabasePool for concurrent reads.
        self.dbQueue = try DatabaseQueue(path: path)

        if let logger {
            self.logger = logger
        } else {
            let dbURL = URL(fileURLWithPath: path)
            let activeLogURL = dbURL.deletingPathExtension().appendingPathExtension("txn.log")
            self.logger = TxnLogger(activeLogURL: activeLogURL)
        }

        do {
            Self.oslog.info("Running migrations")
            try Self.makeMigrator().migrate(dbQueue)
            // Log a basic schema version marker after migration. GRDB migrator doesn't expose version, so we log last migration name.
            Self.oslog.info("Migrations complete", metadata: ["schema": .string("create_kv")])
        } catch {
            Self.oslog.error("Migration failed", metadata: [
                "path": .string(path),
                "error": .string(String(describing: error))
            ])
            throw error
        }
    }

    private static func makeMigrator() -> DatabaseMigrator {
        var m = DatabaseMigrator()
        m.registerMigration("create_kv") { db in
            try db.create(table: "kv_records") { t in
                t.column("record_type", .text).notNull()
                t.column("k", .text).notNull()
                t.column("v", .blob).notNull()
                t.column("updated_at", .double).notNull()
                t.primaryKey(["record_type", "k"])
            }
            try db.create(index: "kv_by_type", on: "kv_records", columns: ["record_type"])
        }
        return m
    }

    @inline(__always)
    private func nowTimestamp() -> TimeInterval { Date().timeIntervalSince1970 }

    private func fetchValueBlob(db: Database, type: String, key: String) throws -> Data? {
        try Row.fetchOne(
            db,
            sql: "SELECT v FROM kv_records WHERE record_type=? AND k=?",
            arguments: [type, key]
        ).flatMap { row in
            try? row.withUnsafeData(named: "v") { data in Data(data!) }
        }
    }

    // MARK: - Operations

    func put(type: String, key: String, value: Data) async throws {
        try await dbQueue.write { db in
            let existing = try fetchValueBlob(db: db, type: type, key: key)
            let now = self.nowTimestamp()
            let txid = UUID().uuidString

            // Log before image for updates
            if let old = existing {
                self.logger.logUpdateBefore(type: type, key: key, ts: now, updatedAt: now, txid: txid, value: old)
            }

            // Perform UPSERT
            try db.execute(
                sql: """
                INSERT INTO kv_records(record_type,k,v,updated_at)
                VALUES (?,?,?,?)
                ON CONFLICT(record_type,k)
                DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at
                """,
                arguments: [type, key, value, now]
            )

            // Log after image (insert or update)
            if existing == nil {
                self.logger.logInsertAfter(type: type, key: key, ts: now, updatedAt: now, txid: txid, value: value)
            } else {
                self.logger.logUpdateAfter(type: type, key: key, ts: now, updatedAt: now, txid: txid, value: value)
            }
        }
    }

    func get(type: String, key: String) async throws -> Data? {
        try await dbQueue.read { db in
            if let row = try Row.fetchOne(
                db,
                sql: "SELECT v FROM kv_records WHERE record_type=? AND k=?",
                arguments: [type, key]
            ) {
                return try row.withUnsafeData(named: "v") { data in
                    Data(data!)   // safe because column is NOT NULL
                }
            }
            return nil
        }
    }

    func delete(type: String, key: String) async throws {
        try await dbQueue.write { db in
            let existing = try fetchValueBlob(db: db, type: type, key: key)
            let now = self.nowTimestamp()
            let txid = UUID().uuidString

            if let old = existing {
                self.logger.logDeleteBefore(type: type, key: key, ts: now, updatedAt: now, txid: txid, value: old)
            } else {
                self.logger.logDeleteBeforeMissing(type: type, key: key, ts: now, updatedAt: now, txid: txid)
            }

            _ = try db.execute(
                sql: "DELETE FROM kv_records WHERE record_type=? AND k=?",
                arguments: [type, key]
            )
        }
    }

    func exists(type: String, key: String) async throws -> Bool {
        try await dbQueue.read { db in
            try Bool.fetchOne(
                db,
                sql: "SELECT EXISTS(SELECT 1 FROM kv_records WHERE record_type=? AND k=?)",
                arguments: [type, key]
            ) ?? false
        }
    }

    func list(type: String, prefix: String?, limit: Int) async throws -> [String] {
        try await dbQueue.read { db in
            if let p = prefix, !p.isEmpty {
                return try String.fetchAll(
                    db,
                    sql: "SELECT k FROM kv_records WHERE record_type=? AND k LIKE ? ORDER BY k LIMIT ?",
                    arguments: [type, "\(p)%", limit]
                )
            } else {
                return try String.fetchAll(
                    db,
                    sql: "SELECT k FROM kv_records WHERE record_type=? ORDER BY k LIMIT ?",
                    arguments: [type, limit]
                )
            }
        }
    }
}

