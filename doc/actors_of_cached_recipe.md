<!-- https://dillinger.io/ -->


---
#### Principal routines
###### `RastersHandler` (one per `DataSource`)
- Periodically called
  - `msg in :` nothing (check for new raster and raster closing)
- `actors out:` actors of new rasters
- `msg out:` kill_this_raster

###### `QueriesHandler` (one per `Raster`)
- Periodically called
  - `msg in :` nothing (check for new queries and space in the output queues)
- Message exchange with the `Producer`. A Query requires several arrays to be built and sent as soon as they are ready.
  - `msg out:` make_those_arrays
  - `msg in :` made_this_array
- Message sent to several actors that may be waiting for the output queue to empty
  - `msg out:` output_queue_update
- Early stopping
  - `msg out:` kill_this_query
  - `msg in :` kill_this_raster

###### `Producer` (one per `Raster`)
- Message exchange with the `QueriesHandler`.
  - `msg in :` make_those_arrays
  - `msg out:` made_this_array
- Message exchange with the `CacheHandler`. A Cache tile has to be written and valid before being read.
  - `msg out:` may_i_read_those_cache_tiles
  - `msg in :` you_may_read_this_subset_of_cache_tiles
- Message exchange with `BuilderBedroom` that receives and `Builder` that answers.
  - `msg out:` build_this_array_when_needed_soon
  - `msg in :` built_this_array
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

###### `CacheHandler` (one per `Raster`)
- Message exchange with the `Producer`
  - `msg in :` may_i_read_those_cache_tiles
  - `msg out:` you_may_read_this_subset_of_cache_tiles
- Message exchange with the `FileHasher`. When a cache tile exist and has not been opened yet, it needs to be validated against.
  - `msg out:` get_the_status_of_this_cache_file
  - `msg in :` got_the_status_of_this_cache_file
- Message exchange with `Computer` that receives and `Writer` that answers.
  - `msg out:` compute_those_tiles
  - `msg in :` wrote_this_cache_tile
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

---
### Cache checking
###### `FileHasher` (one per `Raster`)
- Message exchange with the `CacheHandler`
  - `msg in :` get_the_status_of_this_cache_file
  - `msg out:` got_the_status_of_this_cache_file
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

---
### Cache building
###### `Computer` (one per `Raster`)
- Periodically called
  - `msg in :` nothing (check for new arrays in input queues)
- Carry out request from `CacheHandler` to `ComputeAccumulator->Merger->Writer->CacheHandler` about cache tile creation
  - `msg in :` compute_those_tiles
  - `msg out:` computed_this_tile
- Message exchange with `ComputationBedroom`.  Start building compute tile when it is needed by a production array that fits in output queue.
  - `msg out:` schedule_computation_when_needed_soon
  - `msg in :` schedule_computation
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

###### `ComputationBedroom` (one per `Raster`)
- Message delaying from `Computer` to `Computer` (with updates from `QueriesHandler`)
  - `msg in :` schedule_computation_when_needed_soon
  - `msg in :` output_queue_update
  - `msg out:` schedule_computation
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

###### `ComputeAccumulator` (one per `Raster`)
- Carry out request from `CacheHandler->Computer` to `Merger->Writer->CacheHandler` about cache tile creation
  - `msg in :` computed_this_tile
  - `msg out:` merge_those_tiles
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

###### `Merger` (one per `Raster`)
- Carry out request from `CacheHandler->Computer->ComputeAccumulator` to `Writer->CacheHandler` about cache tile creation
  - `msg in :` merge_those_tiles
  - `msg out:` write_this_cache_tile
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

###### `Writer` (one per `Raster`)
- Carry out request from `CacheHandler->Computer->ComputeAccumulator->Merger` to `CacheHandler` about cache tile creation
  - `msg in :` write_this_cache_tile
  - `msg out:` wrote_this_cache_tile
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

---
### Cache reading
###### `BuilderBedroom`
- Message delaying from `Producer` to `Builder` (with updates from `QueriesHandler`). Start building an array when it fits in output queue. 
  - `msg in :` build_this_array_when_needed_soon
  - `msg in :` output_queue_update
  - `msg out:` build_this_array
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

###### `Builder` (one per `Raster`)
- Carry out request from `Producer->BuilderBedroom` to `Producer` about production array building
  - `msg in :` build_this_array
  - `msg out:` built_this_array
- Message exchange with the `Sampler`. A production array depends on 0 or more cache tiles that need to be read.
  - `msg out:` sample_this_array
  - `msg in :` sampled_this_array
- Message exchange with the `Resampler`. A production array may need to be remapped on a separate thread.
  - `msg out:` resample_this_array
  - `msg in :` resampled_this_array
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

###### `Sampler` (one per `Raster`) (Shares states with an `ActorPool`)
- Message exchange with the `Builder`
  - `msg in :` sample_this_array (put in pool waiting room)
  - `msg out:` sampled_this_array (issued by `ActorPool.receive_nothing()`)
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

###### `Resampler` (one per `Raster`) (Shares states with an `ActorPool`)
- Message exchange with the `Builder`
  - `msg in :` resample_this_array (put in pool waiting room)
  - `msg out:` resampled_this_array (issued by `ActorPool.receive_nothing()`)
- Early stopping
  - `msg in :` kill_this_query
  - `msg in :` kill_this_raster

----

