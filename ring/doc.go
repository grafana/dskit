/*
Package ring contains types and functions for creating and working with rings.

# Overview

Rings are shared between instances via a key-value store, and are represented by the [Desc] struct, which contains a map of [InstanceDesc]
structs representing individual instances in the ring.

# Creating a Ring

Two types are available for creating and updating rings:

  - [Lifecycler] - A BasicService that writes to a ring on behalf of a single instance. It's responsible for claiming tokens on the ring and broadcasting heartbeats, state changes, and token changes. This type is only used for ingester services.
  - [BasicLifecycler] - A BasicService that writes to a ring on behalf of a single instance. It's responsible for claiming tokens on the ring, broadcasting heartbeats and state changes, and uses a delegate with event listeners to help with these. This type is general purpose, is used by numerous services, and is meant for building higher level lifecyclers.

# Observing a ring

The [Ring] type is a BasicService that is primarily used for reading and watching for changes to a ring.
*/
package ring
