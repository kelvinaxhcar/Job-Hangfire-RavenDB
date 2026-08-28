# Contributing to Job.Hangfire.Raven6x

Thank you for your interest in contributing to **Job.Hangfire.Raven6x**! Open-source projects thrive because of community members like you.

The following is a set of guidelines and best practices for contributing to this project.

---

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md). Please report any unacceptable behavior to the project maintainers.

---

## How Can I Contribute?

### 1. Reporting Bugs

Before creating a bug report:
- Search existing [GitHub Issues](https://github.com/kelvinaxhcar/Job-Hangfire-RavenDB/issues) to verify the bug hasn't already been reported.
- Ensure the bug reproduces with the latest version.

When filing a bug report, include:
- A clear, descriptive title.
- Steps to reproduce the issue.
- Expected behavior vs. actual behavior.
- Environment details (.NET version, RavenDB server version, Hangfire version, OS).
- Code snippets, stack traces, or minimal reproduction repository where possible.

### 2. Suggesting Enhancements

Feature requests and performance improvement ideas are always welcome:
- Open an issue describing the proposed feature or improvement.
- Explain the use case and why it would be beneficial to the project.
- Provide example code or API design if applicable.

### 3. Pull Requests

1. **Fork the repository** and create a descriptive branch name from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```
2. **Make your changes**:
   - Adhere to existing coding styles and conventions.
   - Write clean, maintainable code with clear intent.
   - Keep pull requests focused on a single change or feature.
3. **Build and test locally**:
   ```bash
   dotnet build
   dotnet test
   ```
4. **Commit with meaningful commit messages**:
   ```bash
   git commit -m "feat(storage): optimize query batching for dashboard stats"
   ```
5. **Push to your fork** and submit a **Pull Request** targeting the `main` branch.
6. **Participate in code review**: Respond to feedback or suggestions promptly.

---

## Development Setup

### Prerequisites
- [.NET 7.0 / 8.0 / 9.0 SDK](https://dotnet.microsoft.com/download)
- [RavenDB 6.x](https://ravendb.net/download) (Standalone instance or via Docker: `docker run -d -p 8080:8080 ravendb/ravendb:latest`)
- Visual Studio 2022, Rider, or VS Code with C# DevKit

### Solution Structure
- `src/Hangfire.Raven/` – The core RavenDB storage provider library.
- `src/Hangfire.Raven.Tests/` – Unit and integration test suite.
- `samples/` – Example applications demonstrating integration with ASP.NET Core and Console apps.

---

## Coding Standards

- Follow standard C# coding conventions and Microsoft design guidelines.
- Always use `using` or `using var` when opening `IDocumentSession` instances to prevent connection and memory leaks.
- Favor batching (`Lazily()`, `CountLazily()`) and point-lookups (`session.Load<T>(id)`) over unconstrained query scans.
- Use explicit metadata assignments (`this['@metadata'] = { '@collection': '...' }`) when creating documents via RavenDB JavaScript patches.

---

## Questions & Discussions

If you have questions regarding usage or architecture, feel free to open a [GitHub Discussion or Issue](https://github.com/kelvinaxhcar/Job-Hangfire-RavenDB/issues).
