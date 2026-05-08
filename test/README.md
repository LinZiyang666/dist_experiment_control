# Test Workspace

This directory is the default home for project-level tests owned by the test
engineering workflow.

- `e2e/`: end-to-end and black-box tests that run real project binaries and
  external services.
- Package-local Go unit tests may still live next to implementation files when
  they need access to unexported symbols or tight package fixtures.

