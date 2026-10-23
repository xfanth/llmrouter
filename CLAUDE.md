# Rules for working on llmrouter

These rules apply to every change in this repository, by a person or an agent.

## 1. Test-driven development, always

Every change to behaviour is written test first. This follows Kent Beck's
[Canon TDD](https://newsletter.kentbeck.com/p/canon-tdd), with the refactoring
step from Martin Fowler's
[Test Driven Development](https://martinfowler.com/bliki/TestDrivenDevelopment.html).

1. **Write the test list.** Before touching code, write down the cases the
   change must handle: the normal case, the edge cases, and the failure the
   change is meant to fix. List behaviour, not implementation.
2. **Turn one item into a real test.** One test only. It must be concrete and
   runnable, and it must assert the behaviour.
3. **Watch it fail (red).** Run it and see it fail, for the reason you expect.
   A test that passes before the change proves nothing. A bug fix starts with a
   test that reproduces the bug: the reproduction becomes the regression test.
4. **Make it pass (green).** Write the least code that passes this test and
   every earlier one. Add any new cases you discover to the list rather than
   coding them now.
5. **Refactor.** With every test green, clean up the new code and the code
   around it. Fowler: skipping this step is the most common way TDD goes wrong.
6. **Repeat from step 2** until the list is empty.

Do not:

- write a test with no assertion, or one written only for coverage;
- write every test on the list before making any of them pass;
- delete or weaken an assertion to make a test pass;
- copy the code's actual output into the expected value without checking it
  is right;
- refactor while a test is red;
- abstract early. Duplication is a hint, not a command.

In this repo:

- Unit tests sit beside the code (`#[cfg(test)] mod tests`). Behaviour across
  the HTTP edge goes in `tests/integration/`, wire formats in
  `tests/contract/`, and the built Docker image in `tests/smoke/`.
- Timer-driven behaviour uses `#[tokio::test(start_paused = true)]`, so a
  60 second stall runs in milliseconds. A test that has to use the real clock
  leaves a wide margin: the coverage run is instrumented and slow.
- `cargo test` runs one red-green loop. Before a pull request, run the whole
  gate, `./ci.sh` (clippy, the nextest coverage run with its 80% floor, and
  the smoke tier). CI runs exactly the same script.

## 2. After every deployment, prove the fix is live

A deployment is not done when the image is built, or when the container
restarts. It is done when the tests for the issue it fixes pass against the
deployed router.

1. Check the right build is running: `curl http://truenas.arpa:8080/version`
   must show the merge commit.
2. Re-run the reproduction that proved the issue: the same request, script or
   client that failed before the fix, against the deployed router. It must now
   pass.
3. Run the redeploy suite, `scripts/redeploy_suite.py`, against the deployed
   router. Its HTTP tiers need the base URL with `/v1`, and its Claude Code
   tier needs it without, so run it once each way until that is fixed.
4. Read the live counters for the failure the fix targets on `/metrics`,
   `/metrics/requests` and `/healthz`.
5. Report each issue as fixed, not fixed, or not yet shown, with the evidence.
