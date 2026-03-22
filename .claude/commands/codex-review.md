# Codex Review

Review the current staged/uncommitted changes (or a specific commit) using OpenAI's Codex CLI, then evaluate its feedback before committing.

## Arguments

- `$ARGUMENTS` — optional: a commit SHA to review. If omitted, reviews uncommitted changes.

## Instructions

### Step 1: Run Codex Review

Run OpenAI's Codex CLI review command via `npx @openai/codex`.

If `$ARGUMENTS` is a commit SHA, use:

```
echo "<prompt>" | npx @openai/codex review --commit <SHA>
```

If `$ARGUMENTS` is empty, use:

```
echo "<prompt>" | npx @openai/codex review --uncommitted
```

The Codex CLI does not accept a positional prompt together with `--uncommitted` or `--commit` — the prompt must be piped via stdin.

Use this exact prompt for the Codex review:

```
You are a senior staff engineer performing a detailed code review. Analyze every changed file thoroughly. Produce your review in the following structured format:

## CORRECTNESS (Critical)
List issues where the code may produce wrong results, crash, cause data loss, or violate invariants. For each issue:
- **File:line** — description of the bug or logical error
- **Risk**: High / Medium / Low
- **Suggestion**: concrete fix or approach

## PERFORMANCE & EFFICIENCY
List issues where the code may be unnecessarily slow, use excessive memory, or miss obvious optimizations. For each issue:
- **File:line** — description of the inefficiency
- **Risk**: High / Medium / Low
- **Suggestion**: concrete improvement

## CODE QUALITY
List issues with readability, maintainability, naming, structure, idiomatic usage, error handling, and API design. For each issue:
- **File:line** — description of the quality concern
- **Risk**: High / Medium / Low
- **Suggestion**: concrete improvement

## SECURITY
List any potential security issues (injection, unsafe operations, secret exposure, etc). For each issue:
- **File:line** — description of the vulnerability
- **Risk**: High / Medium / Low
- **Suggestion**: concrete fix

## SUMMARY
- Total issues found by category and risk level
- Overall assessment: APPROVE / REQUEST CHANGES / NEEDS DISCUSSION
- Top 3 most important items to address before merging

Within each section, order issues from highest to lowest risk. Be specific — always reference exact file paths and line numbers. If a section has no issues, write "No issues found." Do not pad the review with trivial nitpicks.
```

Set a timeout of 300 seconds (5 minutes) for the command.

### Step 2: Evaluate the Codex Output

After receiving the Codex review output, critically evaluate it:

1. **Verify claims**: For any High or Medium risk issues Codex identified, read the relevant files and line numbers to confirm whether the issues are real or false positives.

2. **Triage**: Categorize each finding as:
   - **Valid & Actionable** — real issue that should be fixed before committing
   - **Valid but Minor** — real issue but acceptable to defer
   - **False Positive** — Codex got it wrong, explain why

3. **Present your evaluation** to the user with this structure:
   - Issues that should be fixed now (with suggested fixes)
   - Issues that are valid but can be deferred
   - False positives dismissed with reasoning

4. **Ask the user** how they'd like to proceed:
   - Fix the actionable issues now
   - Proceed with committing as-is
   - Fix specific items only

Do NOT automatically make changes or commit. Present the evaluation and wait for the user's decision.
