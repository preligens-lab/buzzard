
# Source code organization
All directories and files starting with an `_` are private, the `__init__.py` takes care of exporting all symbols.

---

### `*.py`
##### `__init__.py`
Symbols export

##### `_a_*.py`
Almost all files follow the following semantic: a 'facade' class that starts with `A` and directly or indirectly inherits from `AProxy`, and a 'back' class that starts with `ABack` and directly or indirectly inherits from `ABackProxy`.

##### `_datasource*.py`
Contains the definition of the `DataSource` and `BackDataSource` classes, as well as their mixins.

##### `_env.py`
The `Env` class and `env` singleton.

##### `_footprint*.py`
Contains the definition of the `Footprint` class, as well as some mixins.

##### `_pint_interop.py`
A not so useful file

##### Concrete raster proxies
```
_gdal_file_raster.py
_gdal_mem_raster.py
_numpy_raster.py
_cached_raster_recipe.py
```
All concrete 'facade' that directly or indirectly inherit from `ARasterProxy` (and their 'back' equivalent of course).

##### Concrete raster vectors
```
_gdal_file_vector.py
_gdal_memory_vector.py
```
All concrete 'facade' that directly or indirectly inherit from `AVectorProxy` (and their 'back' equivalent of course).

---

### `_actors`
Contains all the code that runs on the `DatasSource`'s dedicated thread (except for the thread's main loop that lies in `_datasource_back_scheduler.py`). The mission of those files is to make the `ScheduledRaster` instances work. It is implented with a lightweight message passing design really close to the actor model, the main difference is that all actors run in a fully synchronous way, it allows a lot of freedom in the design:
- No stale messages because messages are delivered in a `depth first` fashion.
- Ability to share some mutable states between actors. (This is used only a few time because it tends to make the code unpredictable)
<br/>

To get a quick taste of how it works you should look at those lines of code:
- The full `buzzard/_datasource_back_scheduler.py` file
- All the classes that inherit from `ScheduledRaster` in `buzzard/*.py`.
- The `buzzard/_actors/message.py` file
- At least one actor, for example the `buzzard/_actors/cached/writer.py` that is pretty straightforward and well documented.

##### `_actors/*.py`
Contains:
- all actor classes that are shared between two or more scheduled rasters
- several classes that are passed in messages between actors

##### `_actors/cached/*.py`
Contains code specific to the `CachedRasterRecipe` (`DataSource.create_cached_raster_recipe`)

##### `_actors/nocache/*.py`
Contains code specific to the `NocacheRasterRecipe` (`DataSource.create_raster_recipe`)

##### `_actors/gdal_file/*.py`
Contains code specific to the `GDALScheduledFileRaster` (`DataSource.(open|create)_raster` with `scheduled!=False` parameter)

---

### `_tools`
- All standalone tools that makes implementation easier

##### `_tools/conv/*.py`
Conversions from gdal types and enums to pythonic ones

---

### `srs/*.py`
Mostly unstable stuff dealing with spatial reference, will be reorganized in the future

---

### `algo/*.py`
Mostly unstable stuff, will be reorganized in the future

---

### `test/*.py`
Unit tests.

---
---
