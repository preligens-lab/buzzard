# Source code organization
All directories and files starting with an `_` are private, the `__init__.py` file takes care of exporting all symbols.

---

## `./*.py` files
#### `__init__.py`
Symbols export

#### `_a_*.py`
Almost all files follow this semantic: a `facade` class that starts with `A` and directly or indirectly inherits from `ASource`, and a `back` class that starts with `ABack` and directly or indirectly inherits from `ABackSource`.

> Diagram: https://www.draw.io/#Uhttps%3A%2F%2Fraw.githubusercontent.com%2Fairware%2Fbuzzard%2Fmaster%2Fdoc%2Fuml%2Fdraw-io-classes-doc.xml

#### `_dataset*.py`
Contains the definition of the `Dataset` and `BackDataset` classes, as well as their mixins.

#### `_env.py`
The `Env` class and `env` singleton.

#### `_footprint*.py`
Contains the definition of the `Footprint` class, as well as some mixins.

#### `_pint_interop.py`
A not so useful file

#### Concrete raster sources
```
_gdal_file_raster.py
_gdal_mem_raster.py
_numpy_raster.py
_cached_raster_recipe.py
```
All concrete `facade` that directly or indirectly inherit from `ARasterSource` (and their `back` equivalent of course).

#### Concrete vector sources
```
_gdal_file_vector.py
_gdal_memory_vector.py
```
All concrete `facade` that directly or indirectly inherit from `AVectorSource` (and their `back` equivalent of course).

---

## `_actors` directory
Contains all the code that runs on the `Dataset`'s dedicated thread (the scheduler) (except for the thread's main loop that lies in `_dataset_back_scheduler.py`). The mission of those files is to make the `AsyncRaster` instances work. It is implemented with a lightweight message passing design really close to the actor model, the main difference is that all actors run in a fully synchronous way, it allows a lot of freedom in the design:
- Almost no stale messages because messages are delivered in a `depth first` fashion.
- Ability to share some mutable states between actors. (This is used only a few times because it tends to make the code unpredictable)
<br/>

To get a quick taste of how it works you should look at those lines of code:
- The full `buzzard/_dataset_back_scheduler.py` file
- All the classes that inherit from `AsyncRaster` in `buzzard/*.py`
- The `buzzard/_actors/message.py` file
- At least one actor, for example the `buzzard/_actors/cached/writer.py` that is pretty straightforward and well documented.

> Diagram: https://www.draw.io/#Uhttps%3A%2F%2Fraw.githubusercontent.com%2Fairware%2Fbuzzard%2Fmaster%2Fdoc%2Fuml%2Fdraw-io-cache-actors.xml

#### `_actors/*.py`
Contains:
- all actor classes that are shared between two or more async rasters
- several classes that are passed in messages between actors

#### `_actors/cached/*.py`
Contains the code specific to the `CachedRasterRecipe` (`Dataset.create_cached_raster_recipe`)

#### `_actors/nocache/*.py`
Contains the code specific to the `NocacheRasterRecipe` (`Dataset.create_raster_recipe`)

#### `_actors/gdal_file/*.py`
Contains the code specific to the `AsyncStoredRaster` (`Dataset.(open|create)_raster` with `async_!=False` parameter)

---

## `_tools` directory
- All standalone tools that make implementation easier.

#### `_tools/conv/*.py`
Conversions from gdal types and enums to pythonic ones

---

## `srs/*.py` files
Mostly unstable stuff dealing with spatial reference, will be reorganized in the future

---

## `algo/*.py` files
Mostly unstable stuff, will be reorganized in the future

---

## `utils/*.py` files
Contains utility code for buzzard's users, like pre-built recipes.

---

## `test/*.py` files
Unit tests.

---
---
