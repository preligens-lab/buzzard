# 0.6.5
- `buzzard` is now maintained by `earthcube-lab`
- Drop support for python 3.4 and 3.5, add support for python 3.8

## Public changes
### New features
- Can now decorate python function with an `Env` object
- `fp.erode` and `fp.dilate` now accept more parameters to control the effect on each border
- Add `fp.forward_conv2d`, `fp.backward_conv2d`, `fp.forward_convtranspose2d` and `fp.backward_convtranspose2d` to anticipate the effect of a convolution on the shape of a tensor

### Bug fixes
- Process pools managed by a `Dataset` are now joined without dead-lock (No UT yet for Pool management)
- Fix `Env` to work with python threads (New UT)
- Fix `fp.intersection(rotation='fit')` when `fp` has a non-north-up scale (No UT for non-north-up Footprints)
- Fix bug when using an integer to select a raster band

## Private changes
- Remove version constraint of pytest
- Update CI
- Now using dockerhub instead of aws

---

# 0.6.4
Small bug fix when using numpy scalars

---

# 0.6.3
Buzzard now has a documentation

---


# 0.6.2
## Public changes
### Parameters update
- Add `srs.wkt_of_file@center` now accepts coordinates

---

# 0.6.1
## Public changes
### Footprint coordinates rounding after reprojection
Thanks to the new parameter of the `Footprint.move` method, opening a raster when using an `sr_work` in the `Dataset` constructor works much better than before.

### Parameters update
- Add `Footprint.move@round_coordinates`
## Private changes
- Unit tests now work with `GDAL 3.0`
- Add strong unit tests for `Footprint.move`

---

# 0.6.0 - Many Interface Changes
See https://github.com/airware/buzzard/releases/tag/0.6.0

---

# 0.5.0 - Raster Cached Recipes
See https://github.com/airware/buzzard/releases/tag/0.5.0

---

# v0.5.0b0
See https://github.com/airware/buzzard/releases/tag/v0.5.0b0

---

# 0.4.4
See https://github.com/airware/buzzard/releases/tag/0.4.4

---

# 0.4.3
See https://github.com/airware/buzzard/releases/tag/0.4.3

---

# 0.4.2
See https://github.com/airware/buzzard/releases/tag/0.4.2

---

# 0.4.1 - Minor changes
See https://github.com/airware/buzzard/releases/tag/0.4.1

---

# 0.4.0 - DataSource pickling
See https://github.com/airware/buzzard/releases/tag/0.4.0

---

# 0.3.2
See https://github.com/airware/buzzard/releases/tag/0.3.2

---

# 0.3.1
See https://github.com/airware/buzzard/releases/tag/0.3.1

---

# 0.3.0
See https://github.com/airware/buzzard/releases/tag/0.3.0
