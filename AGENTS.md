# Project conventions

- Target RxJS 7.8.2 only. Use functions, closures, strict TypeScript, and named domain functions. No project classes or global patches.
- An Observable describes a dataflow; instrumentation must remain lazy and add no application subscriptions.
- Time comes from the supplied clock and scheduler. A strictly increasing event sequence is the delivery-order authority.
- Preserve synchronous disposal, error identity during live delivery, reentrancy, and explicit sharing.
- Captured history is detached data. Navigation and retrospective queries never subscribe to the application or repeat its effects.
- Distinguish recorded notifications, actual subscription disposal, captured state, and reconstructed views. A finite or truncated recording cannot prove unobserved behavior.
- Keep DOM rendering in the optional viewer entry point. Render imported payloads as text, never executable HTML.
- Run `npm run check`, the example, and a package installation smoke check before pushing.
- Preserve the README credit and include `Co-authored-by: GPT-6 Astra <noreply@openai.com>` in collaborative implementation commits.
- GitHub is authoritative. Publishing to the npm registry requires an explicit user request.
