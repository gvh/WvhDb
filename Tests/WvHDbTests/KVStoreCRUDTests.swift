//
//  Untitled.swift
//  WvHDb
//
//  Created by Gardner von Holt on 11/11/25.
//

import Foundation
import Testing
@testable import WvHDb // Replace with the module that contains KVStore


// If you want to assert logging calls, use this Spy instead:
/*
 final class SpyTxnLogger: TransactionLogging {
 private(set) var calls: [String] = []
 func logUpdateBefore(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) { calls.append("update-before:\(type):\(key)") }
 func logUpdateAfter(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) { calls.append("update-after:\(type):\(key)") }
 func logInsertAfter(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) { calls.append("insert-after:\(type):\(key)") }
 func logDeleteBefore(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) { calls.append("delete-before:\(type):\(key)") }
 func logDeleteBeforeMissing(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String) { calls.append("delete-before-missing:\(type):\(key)") }
 }
 */

@Suite("KVStore basic CRUD")
struct KVStoreCRUDTests {

    private func makeTempDBURL() throws -> URL {
        let dir = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent("KVStoreTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir.appendingPathComponent("test.sqlite")
    }

    @Test("Insert, Get, Exists, List, Delete happy path")
    func testCRUD() async throws {
        let dbURL = try makeTempDBURL()
        let store = try KVStore(path: dbURL.path, logger: NoOpTxnLogger())

        let type = "users"
        let key = "alice"
        let csv = "id,name\n1,Alice\n"
        let data = Data(csv.utf8)

        // Insert
        try await store.put(type: type, key: key, value: data)

        // Exists
        let exists = try await store.exists(type: type, key: key)
        #expect(exists == true)

        // Get
        let fetched = try await store.get(type: type, key: key)
        let fetchedString = String(data: try #require(fetched), encoding: .utf8)
        #expect(fetchedString == csv)

        // List
        let keysAll = try await store.list(type: type, prefix: (nil as String?), limit: 10)
        #expect(keysAll.contains(key))

        let keysPref = try await store.list(type: type, prefix: "a", limit: 10)
        #expect(keysPref == [key])

        // Update
        let updatedCSV = "id,name\n1,Alice Liddell\n"
        try await store.put(type: type, key: key, value: Data(updatedCSV.utf8))
        let fetchedUpdated = try await store.get(type: type, key: key)
        #expect(String(data: try #require(fetchedUpdated), encoding: .utf8) == updatedCSV)

        // Delete
        try await store.delete(type: type, key: key)
        let existsAfterDelete = try await store.exists(type: type, key: key)
        #expect(existsAfterDelete == false)
    }

    @Test("List limit and prefix behavior")
    func testListBehavior() async throws {
        let dbURL = try makeTempDBURL()
        let store = try KVStore(path: dbURL.path, logger: NoOpTxnLogger())

        let type = "items"
        let values = [
            ("a1", "x,1\n"),
            ("a2", "x,2\n"),
            ("b1", "y,1\n")
        ]
        for (k, v) in values {
            try await store.put(type: type, key: k, value: Data(v.utf8))
        }

        let all = try await store.list(type: type, prefix: (nil as String?), limit: 10)
        #expect(Set(all) == Set(["a1", "a2", "b1"]))

        let prefA = try await store.list(type: type, prefix: "a", limit: 10)
        #expect(Set(prefA) == Set(["a1", "a2"]))

        let limited = try await store.list(type: type, prefix: (nil as String?), limit: 2)
        #expect(limited.count == 2)
    }
}
