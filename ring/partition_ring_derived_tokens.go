package ring

import (
	"flag"
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync"

	"go.uber.org/atomic"
)

// Partition tokens are a pure function of the partition ID: AddPartition has always generated them
// with the spread-minimizing generator, in zone 0, and nothing ever changes them. A partition
// therefore declares the scheme its tokens are generated with instead of gossiping ~2KB of them,
// and the tokens are derived by the members which actually need them.
//
// The work is deliberately split in two:
//
//   - Decode validates: the scheme must be known and the partition ID must be derivable. It doesn't
//     generate anything, so relaying a partition ring costs a map lookup and an integer comparison
//     per partition.
//   - NewPartitionRingWithOptions materializes: it's the only place partition tokens are consumed,
//     so only the members which build a ring ever run the generator.
//
// Decode admits only what materialization can serve, which is what makes materialization
// infallible: it runs in the ring watcher, where an error would kill the watch loop.

const (
	// TokenSchemeSpreadMinimizing512 is the scheme every partition has implicitly been using since
	// partition rings were introduced: 512 tokens from the spread-minimizing generator.
	TokenSchemeSpreadMinimizing512 = "smt-v1-512"

	// spreadMinimizingGeneratorV1 is the generator behind the "smt-v1-*" schemes. Schemes sharing a
	// generator share the memoized tokens: a scheme declaring fewer tokens is served the prefix of
	// the reserved set, which is exactly what GenerateTokens returns for it.
	spreadMinimizingGeneratorV1 = "smt-v1"
)

// partitionTokenScheme is the (generator, tokens count) pair a scheme name maps to. Keeping the
// count in the name makes validating a gossiped value a map lookup, instead of a bounds check on a
// number a peer chose.
type partitionTokenScheme struct {
	generator   string
	tokensCount int
}

var partitionTokenSchemes = map[string]partitionTokenScheme{
	TokenSchemeSpreadMinimizing512: {generator: spreadMinimizingGeneratorV1, tokensCount: optimalTokensPerInstance},
}

// DefaultMaxDerivedPartitionID is the highest partition ID whose tokens are derived. The whole
// range is derived in a single pass when a partition ring watcher starts, and never again: the
// memoized set doesn't grow with the ring, so a ring holding a higher partition ID is rejected
// instead of making the process derive its way to an OOM.
//
// The range costs ~160ms and ~2KB per partition to derive (see BenchmarkGenerateAllTokensUpTo), so
// this default is ~320ms of a watcher's start up and ~4MB retained for the lifetime of the process.
const DefaultMaxDerivedPartitionID = 2048

var (
	maxDerivedPartitionID = atomic.NewInt32(DefaultMaxDerivedPartitionID)
	writtenTokenScheme    = atomic.NewString(TokenSchemeSpreadMinimizing512)
)

// RegisterPartitionTokensFlags registers the flags controlling how the tokens of the partitions
// declaring a token scheme are derived. They configure process-wide state, so they must be
// registered on a single flag set and parsed before any partition ring KV client is used.
func RegisterPartitionTokensFlags(f *flag.FlagSet) {
	f.Var(maxDerivedPartitionIDFlag{}, "partition-ring.max-derived-partition-id", "Highest partition ID whose tokens are derived from the token scheme it declares. The tokens of the whole range are derived when a partition ring watcher starts, at a cost of ~160ms and ~2KB per 1024 partition IDs, and a partition ring holding a higher partition ID is rejected.")
}

// maxDerivedPartitionIDFlag reads the flag into the process-wide setting, which is what both the
// codec and the token memo consult. Nothing holds a per-instance copy of it.
type maxDerivedPartitionIDFlag struct{}

func (maxDerivedPartitionIDFlag) String() string {
	return strconv.Itoa(int(maxDerivedPartitionID.Load()))
}

func (maxDerivedPartitionIDFlag) Set(value string) error {
	maxID, err := strconv.Atoi(value)
	if err != nil {
		return err
	}

	return SetMaxDerivedPartitionID(maxID)
}

// SetMaxDerivedPartitionID sets the highest partition ID whose tokens this process derives, the
// programmatic equivalent of -partition-ring.max-derived-partition-id. Decoding a partition ring
// holding a higher partition ID panics instead, which bounds the work a single gossiped value can
// trigger. It must be called before any partition ring KV client is used.
func SetMaxDerivedPartitionID(maxID int) error {
	if maxID < 0 || maxID > math.MaxInt32 {
		return fmt.Errorf("max derived partition ID %d out of range [0, %d]", maxID, math.MaxInt32)
	}

	maxDerivedPartitionID.Store(int32(maxID))
	return nil
}

// SetPartitionTokenScheme sets the token scheme AddPartition declares for the partitions it
// creates. An empty scheme makes it store the tokens in the partition instead, the way binaries
// predating the token scheme did.
//
// Every member of the gossip cluster must know a scheme before any writer emits it, so this must
// only be changed once the whole cluster runs a binary which knows the new scheme. It must be
// called before any partition ring KV client is used.
func SetPartitionTokenScheme(scheme string) error {
	if _, ok := partitionTokenSchemes[scheme]; !ok && scheme != "" {
		return fmt.Errorf("unknown partition token scheme %q", scheme)
	}

	writtenTokenScheme.Store(scheme)
	return nil
}

// WarmDerivedPartitionTokens derives and memoizes the tokens of every derivable partition, so that
// building the first ring doesn't pay for it. It's called by the partition ring watcher when it
// starts: members which don't watch a partition ring never derive anything.
func WarmDerivedPartitionTokens() error {
	for _, cache := range partitionTokensCaches {
		if _, err := cache.derived(); err != nil {
			return err
		}
	}

	return nil
}

// validatePartitionTokens returns an error if a partition can't be turned into a set of tokens.
//
// A partition either carries its tokens (the representation used before token schemes, still
// accepted and never derived over) or declares the scheme to derive them from. A partition doing
// neither is corrupt: it's the signature of a value relayed by a binary predating the token scheme,
// which drops the field when it re-encodes the value for its peers.
func validatePartitionTokens(id int32, partition PartitionDesc) error {
	if partition.TokenScheme == "" {
		if len(partition.Tokens) == 0 {
			return fmt.Errorf("partition %d carries neither tokens nor a token scheme: the value was likely relayed by a binary predating the token scheme, which drops it", id)
		}
		return nil
	}

	if _, ok := partitionTokenSchemes[partition.TokenScheme]; !ok {
		return fmt.Errorf("partition %d declares unknown token scheme %q: every gossip member must know a token scheme before any writer emits it", id, partition.TokenScheme)
	}

	// The memo covers a fixed range of partition IDs, so a peer gossiping a bogus partition ID
	// can't make this process derive its way to an OOM.
	if maxID := maxDerivedPartitionID.Load(); id < 0 || id > maxID {
		return fmt.Errorf("partition %d declares token scheme %q but its ID is outside the derivable partition ID range [0, %d]: raise -partition-ring.max-derived-partition-id", id, partition.TokenScheme, maxID)
	}

	return nil
}

// validatePartitionRingTokens returns an error for the first partition which can't be turned into a
// set of tokens.
func validatePartitionRingTokens(desc *PartitionRingDesc) error {
	for id, partition := range desc.Partitions {
		if err := validatePartitionTokens(id, partition); err != nil {
			return err
		}
	}

	return nil
}

// materializeDerivedTokens returns the input desc with the tokens of every partition declaring a
// token scheme set, deriving them if they're not memoized yet, and the number of partitions whose
// tokens it derived.
//
// The input partitions map is never modified: PartitionRing documents its desc as an immutable
// snapshot, and the map is shared with the caller. Partitions carrying their own tokens are
// returned untouched, which also makes this idempotent: shuffle sharding re-builds sub-rings out of
// an already materialized desc.
//
// It panics on a partition it can't materialize. Values coming from the KV store have been
// validated at decode, so this only triggers on a desc built in-process.
func materializeDerivedTokens(desc PartitionRingDesc) (PartitionRingDesc, int) {
	var (
		derivedPartitions int
		generators        map[string]struct{}
	)

	for id, partition := range desc.Partitions {
		if partition.TokenScheme == "" || len(partition.Tokens) > 0 {
			continue
		}

		if err := validatePartitionTokens(id, partition); err != nil {
			panic(err)
		}

		if generators == nil {
			generators = map[string]struct{}{}
		}
		generators[partitionTokenSchemes[partition.TokenScheme].generator] = struct{}{}

		derivedPartitions++
	}

	if derivedPartitions == 0 {
		return desc, 0
	}

	tokensByGenerator := make(map[string]map[int32]Tokens, len(generators))
	for generator := range generators {
		tokens, err := partitionTokensCaches[generator].derived()
		if err != nil {
			panic(err)
		}

		tokensByGenerator[generator] = tokens
	}

	materialized := PartitionRingDesc{
		Partitions: make(map[int32]PartitionDesc, len(desc.Partitions)),
		Owners:     desc.Owners,
	}

	for id, partition := range desc.Partitions {
		if partition.TokenScheme != "" && len(partition.Tokens) == 0 {
			scheme := partitionTokenSchemes[partition.TokenScheme]
			tokens := tokensByGenerator[scheme.generator][id]
			if len(tokens) < scheme.tokensCount {
				panic(fmt.Errorf("token scheme %q requires %d tokens but the generator only produced %d for partition %d", partition.TokenScheme, scheme.tokensCount, len(tokens), id))
			}

			// The memoized tokens are shared by every partition ring: cap the slice so that a
			// consumer appending to it can't scribble over the memo.
			partition.Tokens = tokens[:scheme.tokensCount:scheme.tokensCount]
		}

		materialized.Partitions[id] = partition
	}

	return materialized, derivedPartitions
}

// partitionTokensCache memoizes the tokens of the derivable partitions generated by a single
// generator. Partition tokens depend on the partition ID alone, so a single process-wide cache per
// generator serves every partition ring: the tokens of partition N are the same in every ring.
// Memoized slices are shared by all the rings and must be treated as immutable.
type partitionTokensCache struct {
	generate func(maxID int32) (map[int32]Tokens, error)

	mtx    sync.RWMutex
	maxID  int32
	tokens map[int32]Tokens
}

var partitionTokensCaches = map[string]*partitionTokensCache{
	spreadMinimizingGeneratorV1: {generate: GenerateAllTokensUpTo, maxID: -1},
}

// derived returns the tokens of the partitions 0..maxDerivedPartitionID, deriving them in a single
// pass if they're not memoized yet. The whole range is derived at once: the generator computes the
// tokens of every lower partition ID to place the ones of the highest anyway. Because the range is
// fixed, this pass runs once per process, when the partition ring watcher warms the memo up.
//
// The returned map is never modified after being published and can be read without holding the lock.
func (c *partitionTokensCache) derived() (map[int32]Tokens, error) {
	maxID := maxDerivedPartitionID.Load()

	c.mtx.RLock()
	if maxID <= c.maxID {
		defer c.mtx.RUnlock()
		return c.tokens, nil
	}
	c.mtx.RUnlock()

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if maxID <= c.maxID {
		return c.tokens, nil
	}

	tokens, err := c.generate(maxID)
	if err != nil {
		return nil, fmt.Errorf("deriving the tokens of the partitions up to %d: %w", maxID, err)
	}

	c.tokens = tokens
	c.maxID = maxID

	return c.tokens, nil
}

// GenerateAllTokensUpTo returns the tokens of every instance ID from 0 to maxID, in the zone-less
// form partitions use. Generating the whole range costs one pass: the spread-minimizing generator
// computes the tokens of all the lower IDs to place the ones of maxID anyway, but GenerateTokens
// throws them away, which makes deriving a range one ID at a time quadratic.
func GenerateAllTokensUpTo(maxID int32) (map[int32]Tokens, error) {
	generator := NewSpreadMinimizingTokenGeneratorForInstanceAndZoneID("", int(maxID), 0, false)

	tokensByID, err := generator.generateTokensByInstanceID()
	if err != nil {
		return nil, err
	}

	tokens := make(map[int32]Tokens, len(tokensByID))
	for id, instanceTokens := range tokensByID {
		// GenerateTokens returns them sorted, and that's how partitions used to store them.
		slices.Sort(instanceTokens)
		tokens[int32(id)] = instanceTokens
	}

	return tokens, nil
}
