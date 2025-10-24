# Contributing to Erdos Graph

Thank you for your interest in contributing to Erdos Graph! Even considering contributing is very much appreciated! This document provides guidelines and instructions for contributing to the project. 

We're committed to fostering an inclusive, welcoming community where everyone can contribute. Please be respectful, kind, and considerate in all interactions. We're all here to learn and build something great together!

## Table of Contents

- [Getting Started](#getting-started)
  - [Where to Start](#where-to-start)
  - [Prerequisites](#prerequisites)
  - [Building Locally](#building-locally)
  - [Running Tests](#running-tests)
- [Development Workflow](#development-workflow)
  - [Making Changes](#making-changes)
  - [Code Quality](#code-quality)
- [Code Standards](#code-standards)
  - [Writing Tests](#writing-tests)
  - [Code Comments](#code-comments)
  - [Commit Messages](#commit-messages)
- [Submitting Changes](#submitting-changes)
  - [Pull Request Process](#pull-request-process)
  - [PR Guidelines](#pr-guidelines)

## Getting Started

### Where to Start

Not sure where to begin? Here's how to find something to work on:

- **Check the [Issues](../../issues)**: Look for issues labeled `good first issue` - these are great starting points!
- **Bug fixes and small improvements**: Feel free to tackle these ad hoc! If you spot a typo, a small bug, or an opportunity for a minor improvement, go ahead and open a PR.
- **New features**: If you want to add a new feature, **please open an issue first** or reach out to discuss it with the maintainers. This helps ensure:
  - The feature aligns with the project's goals
  - We're not duplicating work
  - We can provide guidance on implementation approach
  - You don't spend time on something that might not be merged

When in doubt, opening an issue to discuss your idea is always welcome!

### Prerequisites

Before you begin, ensure you have the following installed:

- **Rust Toolchain**: This project uses Rust nightly (edition 2024)
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  rustup install nightly
  rustup default nightly
  ```

- **System Dependencies**: Depending on your OS, you may need additional libraries for RocksDB:
  - **Ubuntu/Debian**:
    ```bash
    sudo apt-get install clang libclang-dev
    ```
  - **macOS**:
    ```bash
    brew install llvm
    ```

### Building Locally

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/erdos-graph.git
   cd erdos-graph
   ```

2. **Build the project**:
   ```bash
   cargo build
   ```

3. **Build with optimizations** (release mode):
   ```bash
   cargo build --release
   ```

4. **Run the project**:
   ```bash
   cargo run
   ```

### Running Tests

We maintain a high standard for code quality, including an 80% minimum code coverage requirement.

- **Run all tests**:
  ```bash
  cargo test
  ```

- **Run tests with output**:
  ```bash
  cargo test -- --nocapture
  ```

- **Run specific tests**:
  ```bash
  cargo test test_person_properties
  ```

- **Generate coverage report**:
  ```bash
  # Install cargo-llvm-cov if you haven't already
  cargo install cargo-llvm-cov
  
  # Generate coverage report
  cargo llvm-cov --all-features --workspace --html
  
  # View the report (opens in browser)
  open target/llvm-cov/html/index.html
  ```

- **Check coverage threshold**:
  ```bash
  cargo llvm-cov --all-features --workspace --fail-under-lines 80
  ```

## Development Workflow

### Making Changes

1. **Create a new branch** for your work:
   ```bash
   git checkout -b feature/your-feature-name
   ```
   or for bug fixes:
   ```bash
   git checkout -b fix/issue-description
   ```

2. **Make your changes** following the [Code Standards](#code-standards) below.

3. **Test your changes** thoroughly before committing.

4. **Document your changes** before committing.

### Code Quality

Before submitting your changes, ensure they pass all quality checks:

1. **Format your code**:
   ```bash
   cargo fmt --all
   ```

2. **Run the linter**:
   ```bash
   cargo clippy --all-targets --all-features -- -D warnings
   ```

3. **Run all tests**:
   ```bash
   cargo test
   ```

4. **Check coverage** (must be ≥80%):
   ```bash
   cargo llvm-cov --all-features --workspace --fail-under-lines 80
   ```

## Code Standards

### Writing Tests

All new functionality must include comprehensive unit tests. We follow these principles:

- **Test Coverage**: Maintain at least 80% code coverage
- **Test Location**: Place tests in a `tests` module within the same file or in a separate `tests/` directory
- **Test Naming**: Use descriptive names prefixed with `test_`
- **Assertions**: Use appropriate assertions (`assert!`, `assert_eq!`, `assert_ne!`)

**Example test structure**:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_person_properties() {
        // Arrange: Set up test data
        let props = schema::person_properties();
        
        // Act & Assert: Verify expected behavior
        assert!(props.contains_key("name"));
        assert!(props.contains_key("erdos_number"));
    }

    #[test]
    fn test_publication_properties() {
        let props = schema::publication_properties();
        
        assert!(props.contains_key("title"));
        assert!(props.contains_key("year"));
    }

    #[test]
    #[should_panic(expected = "Invalid ID")]
    fn test_invalid_id_panics() {
        // Test that invalid input produces expected panic
        process_invalid_id("");
    }
}
```

**Testing Guidelines**:
- Write tests for both success and failure cases
- Test edge cases and boundary conditions
- Keep tests focused and independent
- Use descriptive assertion messages when helpful:
  ```rust
  assert!(result.is_ok(), "Expected Ok but got: {:?}", result);
  ```

### Code Comments

Write clear, concise comments that explain *why*, not *what*. The code itself should be self-documenting for the "what".

**Module-level documentation**:
```rust
//! Database schema definitions and type creation utilities.
//!
//! This module provides functions to define property schemas for vertices
//! and edges in the Erdos graph database. It includes schemas for Person,
//! Publication, and the relationships between them.
```

**Function documentation**:
```rust
/// Creates a new Person vertex with the specified properties.
///
/// # Arguments
///
/// * `name` - The full name of the person
/// * `erdos_number` - The person's Erdos number (if known)
///
/// # Returns
///
/// Returns a `Result<Uuid, Error>` containing the UUID of the created vertex.
///
/// # Examples
///
/// ```
/// let person_id = create_person("Paul Erdős", Some(0))?;
/// ```
pub fn create_person(name: &str, erdos_number: Option<u32>) -> Result<Uuid, Error> {
    // Implementation
}
```

**Inline comments** (use sparingly):
```rust
// Calculate Erdos number using BFS traversal.
// We need BFS because we want the shortest path.
let erdos_number = calculate_shortest_path(start, end);

// SAFETY: This is safe because we've verified the pointer is non-null
unsafe { *ptr }

// TODO: Optimize this query for large datasets
let results = run_expensive_query();
```

**When NOT to comment**:
```rust
// BAD: Obvious comment that adds no value
// Increment counter by 1
counter += 1;

// GOOD: No comment needed, code is self-explanatory
counter += 1;
```

### Commit Messages

Commit messages can be pretty casual - write whatever makes sense to you! The important thing is that your commits tell a story of your work in progress.

**Examples of perfectly fine commit messages**:

```
add weighted edges
```

```
fix: erdos number was accepting negative values
```

```
WIP: testing new schema approach
```

```
oops, fix typo in previous commit
```

```
working on #42
```

```
refactor connection pooling
```

**General Tips**:
- Make your message descriptive enough that you'll understand it later
- If you're fixing a bug, briefly mention what was wrong
- Feel free to reference issue numbers if relevant
- Don't stress about the format - we'll clean things up in the PR title

Your commits will be squashed when merged anyway, so focus on making progress!

## Submitting Changes

### Pull Request Process

1. **Update your branch** with the latest changes from main:
   ```bash
   git checkout main
   git pull origin main
   git checkout your-branch
   git rebase main
   ```

2. **Push your changes**:
   ```bash
   git push origin your-branch
   ```

3. **Create a Pull Request** on GitHub:
   - Navigate to the repository on GitHub
   - Click "New Pull Request"
   - Select your branch
   - **Carefully craft your PR title** (this is important! See guidelines below)
   - Fill in the PR template

4. **Address review feedback**:
   - Make requested changes
   - Push additional commits
   - Respond to comments

5. **Squash and merge** after approval:
   - The maintainer will squash and merge your PR
   - Your commits will be combined into a single commit
   - **The PR title becomes the final commit message** - this is why it needs to be good!

### PR Guidelines

**PR Title is IMPORTANT!** Unlike commit messages, PR titles should follow the conventional format strictly. This is what shows up in the git history and changelog.

**Required Format**:
```
<type>(<scope>): <clear description>
```

**Types**:
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring without changing behavior
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `chore`: Build process or auxiliary tool changes
- `ci`: CI/CD configuration changes

**Good PR Title Examples**:
```
feat(db): add support for weighted edges
fix(schema): prevent negative erdos numbers
test(db): add integration tests for vertex creation
docs: update installation instructions
refactor(client): simplify connection pooling logic
```

**Bad PR Title Examples**:
```
Update stuff           ❌ Too vague
fixed a bug            ❌ No type or scope
Added new features     ❌ Wrong tense (should be imperative)
feat: things           ❌ Not descriptive
```

**PR Description Template**:
```markdown
## Description
Brief description of the changes and why they're needed.

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Changes Made
- Specific change 1
- Specific change 2
- Specific change 3

## Testing
- [ ] All existing tests pass
- [ ] Added new tests for the changes
- [ ] Coverage remains ≥80%
- [ ] Manual testing performed

## Checklist
- [ ] My code follows the project's style guidelines
- [ ] I have performed a self-review of my code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] My changes generate no new warnings or errors
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] New and existing unit tests pass locally with my changes

## Related Issues
Closes #(issue number)
Related to #(issue number)

## Screenshots (if applicable)
Add screenshots to help explain your changes.

## Additional Notes
Any additional information that reviewers should know.
```

**What to Expect**:
- CI checks must pass before merging (build, test, lint, format, coverage)
- At least one maintainer approval is required
- Keep PRs focused and reasonably sized (generally between 100 and 500 lines)
- Be responsive to feedback and questions

## Questions or Problems?

- **Bug Reports**: Open an issue with a detailed description and steps to reproduce
- **Feature Requests**: Open an issue explaining the feature and its use case
- **Questions**: Feel free to open a discussion or ask in an issue

Thank you for contributing to Erdos Graph! 🎉

