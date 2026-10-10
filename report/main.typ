// Writing scaffold, not finished report prose.
// Compile: typst compile main.typ
// Or upload this file to a new project in the Typst web editor.
#let drafting = true
#let prompt(body) = if drafting {
  block(width: 100%, inset: 9pt, fill: rgb("f1f4f7"), radius: 3pt)[
    #text(size: 9pt, fill: rgb("465568"))[#body]
  ]
}
#set document(title: "A Decentralised Package Registry Using Kademlia")
#set page(paper: "a4", margin: 25mm, numbering: "1")
#set text(size: 11pt, lang: "en")
#set par(justify: true, leading: 0.65em)
#set heading(numbering: "1.1")

#align(center)[
  #text(size: 20pt, weight: "bold")[A Decentralised Package Registry Using Kademlia]
  #v(8pt)
  Group: [group number] \
  [First author] and [Second author] \
  [Submission date]
  #v(6pt)
  Repository: #link("https://github.com/SebPet-00/D7024e-labs")
]

#prompt[
  *Writing guide — remove before submission*

  Write for a technically educated reader who has not taken the course.
  This structure is a suggestion, not an official template. About eight pages
  is a planning target only. Merge short subsections when useful.
  Treat repository Markdown files as working references; verify their claims.
  Replace placeholders and set drafting to false before exporting.
]

#heading(numbering: none, outlined: false)[Abstract]
#prompt[
  Optional: write this last, in about 100–150 words. Summarise the problem,
  system, two experiments, principal findings, and main limitation.
]

// A contents page is optional for a short report.
// #outline(depth: 2)

= Introduction
#prompt[
  About half a page. Introduce the problem, purpose, and scope of the registry.
  State the two evaluation questions: how lookup work changes with network
  size, and how packet loss affects retrieval success. Introduce the store
  and registry as one system; explain course-specific terminology.
]


= Background
#prompt[
  About three quarters of a page. Explain only the background needed later:
  DHTs, content addressing, XOR distance, buckets, iterative lookup,
  replication, k, and alpha. Distinguish integrity from publisher authenticity.
  Cite the original Kademlia paper and identify relevant departures from it.
]

= System design and implementation
#prompt[
  About two to two and a half pages total. Describe component responsibilities,
  interactions, and decisions. Include an architecture diagram if useful.
  Explain representative request flows rather than listing every source file.
]

== Architecture and Kademlia store
#prompt[
  Connect the CLI, node, routing table, lookup, storage, RPC, and transport.
  Explain joining and refresh, lookup parallelism and termination, verification,
  replication, and transfers to new responsible nodes. Explain simplifications,
  no expiration, and the value-size limit with its justification.
]

== Communication and network abstraction
#prompt[
  Explain UDP, response matching, off-path response-guessing protection,
  timeouts, retries, and duplicate requests. Describe real and simulated
  transports. Name libraries and justify their use, especially near RPC.
  Include analysis and deployment dependencies where relevant.
]

== Package registry
#prompt[
  Explain immutable blobs, signed version records, backward links, and the
  mutable latest pointer. Document ownership verification, signature scheme,
  public-key representation, and version ordering. Explain rejection of
  unauthorised updates, forks, and rollbacks, plus missing-history catch-up.
  Distinguish local consistency checks from network-wide guarantees.
]

== Concurrency and thread safety
#prompt[
  Identify shared state and critical regions. Explain locks, channels, and
  ownership rules using concrete examples, including validation and replacement
  of a registry head and avoiding network I/O under locks. Race-detector
  results support this explanation but do not replace it.
]

= Verification
#prompt[
  About half a page. Summarise meaningful functional and failure tests,
  including registry validation and concurrency. Report verified coverage and
  race-test results for the submitted implementation. Describe evidence for
  1,000-node simulated tests and 50-container operation, including limitations.
  Do not copy old pass/coverage claims without checking them.
]

= Experimental method
#prompt[About one page. Give enough detail to reproduce and assess both experiments.]

== Setup and workload
#prompt[
  Identify the selected run and implementation version. Use a compact settings
  table: network sizes, loss probabilities, seeds, queries per seed, k, alpha,
  timeouts, retries, latency, value size, and maintenance periods.
  Explain random addresses and values, full joins, loss-free setup, non-holder
  readers, no churn, and controlled variables. Explain the limits of seeded
  repeatability with concurrent execution.
]

== Measurements and analysis
#prompt[
  Define probes, transmission attempts, parallel rounds, and success.
  Explain independent correctness checks, structured logs, and external analysis.
  State whether failed lookups are included. Define the averaging unit, sample
  variance across seed means, and standard-deviation error bars.
  Explain measurement credibility and logging/simulator limitations.
  Do not claim validated latency measurements merely because elapsed time is logged.
]

= Results and interpretation
#prompt[
  About one and a half to two pages including figures. For each experiment:
  expectation and reason, observations, comparison, explanation of deviations.
  Include numerical variance in a compact table or appendix as well as error bars.
]

== Lookup scalability
#prompt[
  Insert probe count versus network size. Explain the expected logarithmic
  trend and distinguish probes from rounds. State how the reference curve is
  constructed: an anchored curve is not an exact theoretical prediction.
  Discuss fixed work associated with k, variability, and correctness.
  Avoid claiming an asymptotic proof from a finite range.
]

// Copy scale.svg from the selected run into report/figures/ to enable this.
// Upload that file too if using the web editor.
// #figure(
//   image("figures/scale.svg", width: 100%),
//   caption: [Describe conditions, measured quantity, and error bars.],
// ) <fig-scale>
// Refer to a labelled figure in prose using @fig-scale.

== Lookup reliability under packet loss
#prompt[
  Insert success rate versus loss. Explain expected effects of request/reply
  loss, retries, parallelism, and replicas. Discuss observations and the lookup
  deadline. Distinguish per-RPC success from whole-lookup success.
  A 100% observed rate does not establish guaranteed success.
]

// #figure(
//   image("figures/loss.svg", width: 100%),
//   caption: [Define success, conditions, and error bars.],
// ) <fig-loss>

= Limitations and improvements
#prompt[
  About half to three quarters of a page. Separate implementation limitations
  from limits of the evidence. Consider size limits, persistence, ownership
  verification, consistency, finite samples, simulated latency, queue drops,
  scheduling, and untested churn. Prioritise improvements and explain their
  purpose. Identify any features beyond minimum requirements and why you added them.
]

= Conclusion
#prompt[
  About a quarter of a page. Answer the evaluation questions and summarise
  what the system achieves within its limits. Introduce no new results.
]

#heading(numbering: none)[References]
#prompt[
  Cite sources where used. Include the original Kademlia paper and other
  technical sources actually consulted. For an automatic bibliography, add
  a BibLaTeX (.bib) or Hayagriva (.yml) file and replace this heading and
  prompt with the bibliography function below.
]
// Once references.bib exists, cite entries using @entry-key and enable:
// #bibliography("references.bib", style: "ieee")

// Optional appendix: full variance tables and reproduction commands.
// Keep essential methodology, results, and reasoning in the main text.

#prompt[
  *Submission checklist — remove before submission*

  - Group number, authors, and accessible repository link.
  - Architecture, implementation, libraries and their reasons.
  - Value-size limit and justification; registry cryptographic choices.
  - Critical regions and thread-safety explanation.
  - Both mandatory experiments; random inputs, seeds, and repeated runs.
  - Averages and variance; definitions and credible methodology.
  - Expectations, observed results, and explanations of deviations.
  - Timeout/retry policy; limitations, improvements, and extra features.
  - Updated figures and references; all claims checked against evidence.
  - Remove placeholders, disable prompts, and inspect the exported PDF.
  - Upload to Canvas before the final sprint review.
]
