# cuda-event

Event system — pub/sub, event sourcing, replay, dead letter queue (Rust)

Part of the Cocapn fleet — a Lucineer vessel component.

## What It Does

### Key Types

- `Event` — core data structure
- `Subscriber` — core data structure
- `DeadLetter` — core data structure
- `EventBus` — core data structure
- `EventStore` — core data structure

## Quick Start

```bash
# Clone
git clone https://github.com/Lucineer/cuda-event.git
cd cuda-event

# Build
cargo build

# Run tests
cargo test
```

## Usage

```rust
use cuda_event::*;

// See src/lib.rs for full API
// 10 unit tests included
```

### Available Implementations

- `Event` — see source for methods
- `EventBus` — see source for methods
- `EventStore` — see source for methods

## Testing

```bash
cargo test
```

10 unit tests covering core functionality.

## Architecture

This crate is part of the **Cocapn Fleet** — a git-native multi-agent ecosystem.

- **Category**: other
- **Language**: Rust
- **Dependencies**: See `Cargo.toml`
- **Status**: Active development

## Related Crates


## Fleet Position

```
Casey (Captain)
├── JetsonClaw1 (Lucineer realm — hardware, low-level systems, fleet infrastructure)
├── Oracle1 (SuperInstance — lighthouse, architecture, consensus)
└── Babel (SuperInstance — multilingual scout)
```

## Contributing

This is a fleet vessel component. Fork it, improve it, push a bottle to `message-in-a-bottle/for-jetsonclaw1/`.

## License

MIT

---

*Built by JetsonClaw1 — part of the Cocapn fleet*
*See [cocapn-fleet-readme](https://github.com/Lucineer/cocapn-fleet-readme) for the full fleet roadmap*
