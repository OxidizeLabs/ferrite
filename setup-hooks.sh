#!/bin/bash

# TKDB Pre-commit Hooks Setup Script
# This script installs pre-commit hooks using the pre-commit framework

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[SETUP]${NC} $1"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

echo "🔧 Setting up pre-commit hooks for TKDB..."

# Check if we're in a Git repository
if [ ! -d ".git" ]; then
    print_error "This doesn't appear to be a Git repository!"
    echo "Please run this script from the root of your TKDB project."
    exit 1
fi

# Check if Cargo.toml exists
if [ ! -f "Cargo.toml" ]; then
    print_error "Cargo.toml not found!"
    echo "Please run this script from the root of your TKDB project."
    exit 1
fi

# Check if .pre-commit-config.yaml exists
if [ ! -f ".pre-commit-config.yaml" ]; then
    print_error ".pre-commit-config.yaml not found!"
    echo "The pre-commit configuration file is missing."
    exit 1
fi

print_status "Found Git repository, Cargo.toml, and pre-commit configuration"

# Check if pre-commit is installed
if ! command -v pre-commit &> /dev/null; then
    print_error "pre-commit is not installed!"
    echo ""
    echo "Please install pre-commit using one of these methods:"
    echo ""
    echo "  Using pip:"
    echo "    pip install pre-commit"
    echo ""
    echo "  Using brew (macOS):"
    echo "    brew install pre-commit"
    echo ""
    echo "  Using conda:"
    echo "    conda install -c conda-forge pre-commit"
    echo ""
    echo "For more installation options, see: https://pre-commit.com/#installation"
    exit 1
fi

print_success "pre-commit is installed"

# Check for required Rust tools
print_status "Checking for required Rust tools..."

# Check for rustfmt
if ! command -v rustfmt &> /dev/null; then
    print_error "rustfmt not found!"
    echo "Please install rustfmt with: rustup component add rustfmt"
    exit 1
fi
print_success "rustfmt found"

# Check for clippy
if ! command -v cargo-clippy &> /dev/null; then
    print_error "clippy not found!"
    echo "Please install clippy with: rustup component add clippy"
    exit 1
fi
print_success "clippy found"

# Optional tools
if ! command -v cargo-audit &> /dev/null; then
    print_warning "cargo-audit not found (optional)"
    echo "Consider installing it with: cargo install cargo-audit"
    echo "This will enable security vulnerability checks."
else
    print_success "cargo-audit found"
fi

# Install pre-commit hooks
print_status "Installing pre-commit hooks..."
if pre-commit install; then
    print_success "Pre-commit hooks installed successfully"
else
    print_error "Failed to install pre-commit hooks"
    exit 1
fi

# Install pre-push hooks (for comprehensive testing)
print_status "Installing pre-push hooks..."
if pre-commit install --hook-type pre-push; then
    print_success "Pre-push hooks installed successfully"
else
    print_warning "Failed to install pre-push hooks (non-critical)"
fi

# Test the configuration
print_status "Testing pre-commit configuration..."
if pre-commit run --all-files --show-diff-on-failure; then
    print_success "All pre-commit checks passed!"
else
    print_warning "Some pre-commit checks failed - this is normal for the first run"
    echo "The hooks will automatically fix many issues on subsequent commits."
fi

echo ""
echo -e "${GREEN}🎉 Pre-commit hooks successfully installed!${NC}"
echo ""
echo "What was installed:"
echo "  • Pre-commit hooks using .pre-commit-config.yaml configuration"
echo "  • Hooks run automatically on git commit and git push"
echo ""
echo "Available hooks:"
echo "  • Code formatting (rustfmt)"
echo "  • Linting (clippy)"
echo "  • Compilation checks"
echo "  • Unit tests"
echo "  • Security audit (if cargo-audit is installed)"
echo "  • General file checks (trailing whitespace, large files, etc.)"
echo ""
echo "Useful commands:"
echo "  • Run hooks manually: pre-commit run --all-files"
echo "  • Update hooks: pre-commit autoupdate"
echo "  • Bypass hooks (not recommended): git commit --no-verify"
echo ""
echo "To modify hook configuration, edit .pre-commit-config.yaml"
echo ""
print_success "Setup complete! Happy coding! 🚀"
