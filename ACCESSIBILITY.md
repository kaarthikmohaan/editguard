# Accessibility

EditGuard is a data pipeline. Today people use it through the command line, `make` targets and Markdown documentation; it has no web interface yet.

## Commitments

- Documentation is plain Markdown with real headings, text alternatives for diagrams, and tables with header rows, so it works with screen readers and GitHub's reader modes.
- Command output is plain text and never relies on colour alone.
- The triage page planned for milestone M5 will be keyboard operable, meet WCAG 2.2 AA contrast, label every control and announce updates to screen readers. It will be checked with VoiceOver before release.

## Known limitations

- Architecture diagrams are Mermaid or ASCII art; each has a text description nearby, but a screen reader may read the ASCII art character by character.
- The Airflow and Spark web UIs used during operation are third-party and outside this project's control.

## Reporting a barrier

Open an issue with the "Bug" form and describe what you tried, what happened, and the assistive technology you use. Accessibility reports are treated as bugs.
