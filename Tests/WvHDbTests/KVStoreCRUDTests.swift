import Foundation
import Testing
@testable import WvHDb

final class NoOpTxnLogger: TransactionLogging {
    func logUpdateBefore(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {}
    func logUpdateAfter(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {}
    func logInsertAfter(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {}
    func logDeleteBefore(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {}
    func logDeleteBeforeMissing(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String) {}
}

@Suite("KVStore CRUD and query behavior")
struct KVStoreCRUDTests {

    private func makeTempDBURL() throws -> URL {
        let dir = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent("KVStoreTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir.appendingPathComponent("test.sqlite")
    }

    @Test("Insert, Get, Exists, List, Update, Delete")
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

    @Test("List prefix and limit clamping")
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

        // Limit clamping behavior (upper bound in code is 1000, but test a small value)
        let limited = try await store.list(type: type, prefix: (nil as String?), limit: 2)
        #expect(limited.count == 2)
    }

    @Test("Get and delete on missing keys")
    func testMissingKeys() async throws {
        let dbURL = try makeTempDBURL()
        let store = try KVStore(path: dbURL.path, logger: NoOpTxnLogger())

        let type = "ghosts"
        let key = "phantom"

        // Exists should be false
        let exists = try await store.exists(type: type, key: key)
        #expect(exists == false)

        // Get should be nil
        let fetched = try await store.get(type: type, key: key)
        #expect(fetched == nil)

        // Delete should not throw
        try await store.delete(type: type, key: key)
        let existsAfter = try await store.exists(type: type, key: key)
        #expect(existsAfter == false)
    }
}
