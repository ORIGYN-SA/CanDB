module.exports = {
  preset: "ts-jest",
  verbose: true,
  // `maxWorkers: 1` runs all tests serially (not in parallel) to avoid test conflicts.
  // Note: This is the same as running `jest --runInBand` via the CLI
  maxWorkers: 1,
  testEnvironment: "node",
  testTimeout: 360_000,
  testPathIgnorePatterns: ["/node_modules/"],
};