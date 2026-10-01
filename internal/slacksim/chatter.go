package slacksim

import (
	"fmt"
	"strings"
)

// Engineering-chatter templates. {slot}s are filled from the vocab below;
// each channel topic adds its own lines so private channels carry words that
// public ones do not (the leak tests search for them).
var (
	common = []string{
		"{svc} p99 latency jumped to {num}ms in {region} after the {version} rollout, rolling back",
		"postmortem for the {svc} outage is up, root cause was {cause}",
		"PR to bump {dep} to {version} is ready for review, ping me if you have cycles",
		"flaky test in {component} again, filed {ticket} so we stop retrying it",
		"is the {feature} change behind a flag or already on main?",
		"heads up: {svc} deploy freeze starts friday, exceptions go through {handle}",
		"seeing {pct}% error rate on {svc} {endpoint}, anyone else?",
		"benchmarks for {component} look good, {pct}% fewer allocations after the refactor",
		"who owns {svc}? the runbook link in the alert is dead",
		"can someone sanity check my reading of {ticket}? I think it is a dup",
		"{handle} the dashboard for {svc} {metric} is finally fixed",
		"we hit the {limit} limit on {svc} again during the backfill",
		"draft design doc for {feature} is in the shared drive, comments welcome",
		"retro notes: {cause} took most of the week, need better alerts on {metric}",
	}
	byTopic = map[string][]string{
		"kafka": {
			"consumer lag on {topic} keeps climbing, looks like a rebalance storm after the broker restart",
			"anyone tuned fetch.max.wait for low latency consumers? we are at {num}ms end to end",
			"KIP for the new group protocol would fix our {cause} issue",
			"{topic} is compacted now, min.compaction.lag is {num}s",
		},
		"k8s": {
			"node {node} is NotReady again, kubelet logs show {cause}",
			"PDB on {svc} blocked the drain, who set maxUnavailable to 0?",
			"cgroup v2 migration for the {region} pool is done, {num} nodes left",
		},
		"go": {
			"go 1.26 escape analysis change cut {pct}% of heap in {component}",
			"race detector caught a data race in {component}, fix incoming",
			"anyone using iter.Seq in hot paths? curious about the overhead",
		},
		"frontend": {
			"bundle size for the search page went up {pct}% after adding the chart lib",
			"hydration mismatch on {feature}, only in safari",
		},
		"search": {
			"nDCG@5 on the eval set moved from 0.{num} after the analyzer change",
			"hybrid retrieval beats bm25 on long tail queries, rrf k=60 for now",
			"query {query} returns stale results, checking freshness on the connector",
		},
		"data": {
			"spark job for {svc} spilled {num}GB, bumping executor memory",
			"flink checkpoint times are up to {num}s on the {topic} job",
		},
		"incident": {
			"SEV2 declared for {svc}, incident channel is up, {handle} is IC",
			"mitigated: {svc} traffic shifted out of {region}, monitoring",
			"customer impact for the {svc} incident was {num} minutes",
		},
		"deploy": {
			"{svc} {version} is rolling out to {region}, canary looks healthy",
			"rollback of {svc} {version} complete, cause was {cause}",
		},
		"security": {
			"CVE-2026-{num} affects our {dep} version, patch window is tonight",
			"rotated the {svc} signing keys after the leaked token report",
			"pentest finding: {endpoint} on {svc} missing authz check, fix in review",
			"tabletop exercise for credential stuffing on {svc} is thursday",
		},
		"payments": {
			"chargeback spike from {region} merchants, fraud rules updated",
			"ledger reconciliation off by {num} cents for {svc}, investigating",
		},
		"perf": {
			"calibration packets for {team} are due next week",
			"promo case for {handle} needs one more peer review",
		},
		"falcon": {
			"project falcon diligence call moved to tuesday, keep this channel private",
			"falcon integration plan: migrate their connectors onto our indexer in Q3",
			"falcon term sheet comments are in the data room",
		},
		"escalation": {
			"enterprise customer reports stale results from the {svc} connector, escalating",
			"customer escalation on permissions: user saw a doc after leaving the team, checking revoke latency",
		},
		"general": {"office is closed monday for the holiday", "all hands slides are in the drive"},
		"random":  {"who wants to do lunch at the ramen place", "the coffee machine on 3 is fixed"},
		"eng":     {"eng all hands moved to thursday, agenda: {feature} and the {svc} migration"},
	}
	fixups = []string{"typo", "added link", "wrong ticket", "clarified", "updated numbers"}

	vocab = map[string][]string{
		"svc":       {"search-api", "indexer", "auth", "billing", "gateway", "connector-slack", "connector-jira", "ranker", "embedder", "notifications", "ledger"},
		"region":    {"us-east1", "us-west2", "europe-west4", "asia-southeast1"},
		"version":   {"v2.14.0", "v2.15.1", "v3.0.0-rc2", "v1.9.7"},
		"cause":     {"a bad config push", "connection pool exhaustion", "a hot partition", "an expired cert", "GC pauses", "a retry storm", "a schema migration lock"},
		"dep":       {"grpc-go", "franz-go", "opensearch", "react", "openssl", "redis"},
		"component": {"the batcher", "the query planner", "the ACL filter", "the webhook receiver", "the snippet builder"},
		"ticket":    {"KAFKA-17311", "FLINK-36920", "golang/go#70233", "kubernetes#128450", "SPARK-50125"},
		"feature":   {"query suggestions", "people search", "answer citations", "permission explanations", "the new ranking model"},
		"handle":    {"@alice", "@bob", "@erin", "@carol"},
		"pct":       {"3", "12", "27", "40", "65"},
		"num":       {"12", "48", "250", "900", "1500", "37"},
		"endpoint":  {"/v1/search", "/v1/chat", "/admin/export", "/v1/documents"},
		"metric":    {"consumer lag", "p99 latency", "error budget", "freshness"},
		"limit":     {"rate", "memory", "file descriptor", "connection"},
		"topic":     {"ss.docs", "orders.events", "audit.log", "clickstream"},
		"node":      {"gke-pool-a-7f3k", "gke-pool-b-2x9q"},
		"team":      {"platform", "search", "security"},
		"query":     {"\"oncall rotation\"", "\"q3 roadmap\"", "\"expense policy\""},
	}
)

func (w *Workspace) pick(list []string) string { return list[w.rng.IntN(len(list))] }

// say produces one message for a channel.
func (w *Workspace) say(c *Channel) string {
	lines := byTopic[c.Topic]
	tmpl := ""
	if len(lines) > 0 && w.rng.IntN(10) < 6 {
		tmpl = w.pick(lines)
	} else {
		tmpl = w.pick(common)
	}
	var b strings.Builder
	for {
		i := strings.IndexByte(tmpl, '{')
		if i < 0 {
			b.WriteString(tmpl)
			break
		}
		j := strings.IndexByte(tmpl[i:], '}')
		if j < 0 {
			b.WriteString(tmpl)
			break
		}
		b.WriteString(tmpl[:i])
		slot := tmpl[i+1 : i+j]
		if vals, ok := vocab[slot]; ok {
			b.WriteString(w.pick(vals))
		} else {
			fmt.Fprintf(&b, "{%s}", slot)
		}
		tmpl = tmpl[i+j+1:]
	}
	return b.String()
}
