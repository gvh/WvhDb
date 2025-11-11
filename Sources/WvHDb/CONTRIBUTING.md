# Contributing / House Rules

## Editing style
 - Always provide full-file replacements for any file you modify.
 - Batch multi-file edits into a single change whenever practical.
 - Keep changes minimal and focused on the requested scope (no speculative refactors).

### Swift specifics
 - When intentionally discarding the return value of a throwing call, write `_ = try? ...` (or `_ = try ...`) to make the intent explicit and silence unused-result warnings. Example:
   
   ```swift
   _ = try? handle?.seekToEnd()
