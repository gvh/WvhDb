// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "WvHDb",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .executable(name: "WvHDb", targets: ["WvHDb"])
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.17.0"),
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "6.29.3")
    ],
    targets: [
        .executableTarget(
            name: "WvHDb",
            dependencies: [
                .product(name: "Hummingbird", package: "hummingbird"),
                .product(name: "GRDB", package: "GRDB.swift")
            ],
            path: "Sources/WvHDb",
            exclude: [
                "CONTRIBUTING.md"   // or "Contributing.md" depending on actual case
            ]
        ),
        .testTarget(
            name: "WvHDbTests",
            dependencies: ["WvHDb"],
            path: "Tests/WvHDbTests"
        )
    ]
)
