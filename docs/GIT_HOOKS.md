# Pre-commit Hooks for TKDB

This document describes the pre-commit hooks set up for the TKDB project to maintain code quality and prevent broken commits. We use the [pre-commit framework](https://pre-commit.com/) for better maintainability and tooling support.

## Quick Setup

1. Install pre-commit:
   ```bash
   pip install pre-commit
   # OR
   brew install pre-commit
   ```

2. Run the setup script to install all hooks:
   ```bash
   ./setup-hooks.sh
   ```

## Available Hooks

### Pre-commit Hook

The pre-commit hook runs **fast checks** before allowing commits:

1. **Code Formatting** (`cargo fmt --check`)
   - Ensures consistent code style across the project
   - Fails if code is not properly formatted

2. **Linting** (`cargo clippy`)
   - Catches common mistakes and suggests improvements
   - Fails on any clippy warnings

3. **Compilation Check** (`cargo check`)
   - Ensures code compiles without errors
   - Checks all targets and features

4. **Unit Tests** (`cargo test --lib --bins --tests`)
   - Runs fast unit tests only (excludes slow integration tests)
   - Ensures core functionality works

5. **Database-specific Checks**
   - Warns about `.unwrap()` usage (should use proper error handling)
   - Warns about `println!` usage (should use logging)

### Pre-push Hook

The pre-push hook runs **comprehensive checks** before allowing pushes:

1. **Full Test Suite** (`cargo test --all`)
   - Runs all tests including integration tests
   - More thorough than pre-commit testing

2. **Security Audit** (`cargo audit`, if installed)
   - Checks for known security vulnerabilities
   - Only warns, doesn't block pushes

3. **Documentation Build** (`cargo doc`)
   - Ensures documentation builds correctly
   - Catches documentation errors

4. **Database-specific Validations**
   - Checks for potential resource leaks in buffer pool operations
   - Validates transaction management patterns

5. **Release Build** (`cargo build --release`)
   - Ensures code builds in optimized release mode
   - Catches release-specific compilation issues

## Configuration

All hooks are configured in `.pre-commit-config.yaml`. This YAML file defines:

- Which hooks to run
- When to run them (commit vs push)
- Hook-specific configuration
- Repository sources for hooks

To modify hook behavior, edit this file and run:
```bash
pre-commit install  # Reinstall hooks after config changes
```

## Manual Hook Execution

Run hooks manually without committing:

```bash
# Run all hooks on all files
pre-commit run --all-files

# Run specific hook
pre-commit run cargo-fmt

# Run hooks on specific files
pre-commit run --files src/main.rs src/lib.rs
```

## Bypassing Hooks

**⚠️ Not recommended**, but you can temporarily bypass hooks:

```bash
# Skip pre-commit hooks
git commit --no-verify

# Skip pre-push hooks
git push --no-verify
```

## Hook Configuration

### Required Tools

The hooks require these tools to be installed:

- `rustfmt` - Install with: `rustup component add rustfmt`
- `clippy` - Install with: `rustup component add clippy`

### Optional Tools

- `cargo-audit` - Install with: `cargo install cargo-audit`
  - Enables security vulnerability scanning
  - Recommended for production deployments

## Troubleshooting

### Hook Not Running

If hooks aren't running, check:

1. Are the hook files executable? Run: `ls -la .git/hooks/`
2. Are you in the project root? The hooks expect `Cargo.toml` to exist
3. Re-run the setup script: `./setup-hooks.sh`

### Formatting Issues

If you get formatting errors:

```bash
# Fix formatting automatically
cargo fmt

# Then try committing again
git commit -m "Your commit message"
```

### Clippy Issues

If you get clippy warnings:

```bash
# See all clippy suggestions
cargo clippy --all-targets --all-features

# Fix issues and commit again
```

### Test Failures

If tests fail:

```bash
# Run tests locally to see detailed output
cargo test --all --verbose

# Fix failing tests and commit again
```

## Performance Considerations

- **Pre-commit**: Designed to be fast (< 30 seconds for typical commits)
- **Pre-push**: More comprehensive but slower (1-3 minutes depending on test suite size)

The hooks balance thoroughness with developer productivity by running quick checks on commit and comprehensive checks on push.

## Database-Specific Guidelines

The hooks include special checks for database code:

### Error Handling
- Avoid `.unwrap()` in production code
- Use `Result<T, DBError>` return types
- Implement proper error conversion chains

### Resource Management
- Ensure buffer pool pages are properly pinned/unpinned
- Implement `Drop` traits for cleanup when needed
- Check for potential memory leaks in long-running operations

### Transaction Safety
- Always handle transaction completion (commit/rollback)
- Use proper isolation levels
- Test concurrent access scenarios

## Customization

To modify the hooks:

1. Edit the hook files in `.git/hooks/`
2. Or modify the setup script and re-run it
3. The hooks are standard shell scripts and can be customized per project needs

## Integration with CI

The hooks complement the CI pipeline:

- **Local hooks**: Fast feedback during development
- **CI pipeline**: Additional checks on pull requests and main branch

This ensures code quality at multiple levels without slowing down the development workflow.

## Advantages of Pre-commit Framework

Using the pre-commit framework instead of custom shell scripts provides several benefits:

### Better Tooling
- **Automatic tool management**: Pre-commit handles installation and updates of hook dependencies
- **Language support**: Built-in support for many languages and tools
- **Caching**: Intelligent caching speeds up subsequent runs

### Maintainability
- **YAML configuration**: Easy to read and modify `.pre-commit-config.yaml`
- **Version pinning**: Explicit version control for all hooks
- **Community hooks**: Access to pre-built hooks from the community

### Developer Experience
- **Manual execution**: Run hooks without committing using `pre-commit run`
- **Selective execution**: Run specific hooks or on specific files
- **Auto-fixing**: Many hooks can automatically fix issues they find

## Updating Hooks

Keep your hooks up to date:

```bash
# Update all hooks to their latest versions
pre-commit autoupdate

# Update specific hook versions in .pre-commit-config.yaml manually
# Then reinstall
pre-commit install
```

## Advanced Usage

### Running Hooks in CI

The same hooks can run in CI using:

```yaml
# In .github/workflows/ci.yml
- name: Run pre-commit hooks
  uses: pre-commit/action@v3.0.1
```

### Custom Hooks

Add project-specific hooks in `.pre-commit-config.yaml`:

```yaml
- repo: local
  hooks:
    - id: my-custom-check
      name: My Custom Check
      entry: ./scripts/my-check.sh
      language: system
      types: [rust]
```
