# UHI-Barcelona

Final degree project — Geography (TFG)  
Author: rogerespinosamorros  
Date: 2025

---

## Overview

UHI-Barcelona is a reproducible analysis and report package for studying the Urban Heat Island (UHI) in Barcelona. The project combines meteorological station observations and satellite-derived indices (e.g., NDVI, NDBI, LST) to:

- quantify spatial and temporal variability of air and surface temperatures,
- explore relationships between land cover (vegetation, built-up areas) and temperature,
- produce maps, plots and a synthesis for the final thesis.

This repository contains datasets (small/derived), analysis notebooks, scripts, and final reports produced during the project.

---

## Repository structure

- data/  
  - Raw and processed data used in analyses (not all raw data is included in the repo; see data README or links).
- notebooks/  
  - `uhi_satellite_ndvi_ndbi_lst.ipynb` — satellite processing, indices and LST extraction  
  - `analisi_temp_estacions.ipynb` — station temperature analysis and time series  
  - `analisi_hum.ipynb` — humidity analysis  
  - `analisi_pres.ipynb` — pressure analysis  
  - `analisi_sol.ipynb` — solar radiation / insolation analysis  
  - `discussions.ipynb` — notes and interpretation for the thesis
- reports/  
  - ATD/, humidity/, pressure/, satelit_info/, sol/, synthesis/, temp/, uhi_climatology/ — generated figures, tables and PDFs for each analysis block
- scripts/  
  - Utilities, data processing and helper scripts used in notebooks
- src/  
  - Reusable functions and modules
- environment.yml  
  - Conda environment definition used for reproducibility
- requirements.txt  
  - Pinning of Python packages (if you prefer pip)
- README.md
- .gitignore

(See the Explorer screenshot in the repo for an example layout.)

---

## Key files and notebooks (recommended order)

1. notebooks/uhi_satellite_ndvi_ndbi_lst.ipynb  
   - Preprocessing of satellite imagery; compute NDVI, NDBI and LST; export rasters and summary tables.
2. notebooks/analisi_temp_estacions.ipynb  
   - Clean and explore meteorological stations; compute UHI metrics from station network.
3. notebooks/analisi_hum.ipynb, analisi_pres.ipynb, analisi_sol.ipynb  
   - Complementary environmental variables (humidity, pressure, solar input).
4. notebooks/discussions.ipynb  
   - Synthesis of results and interpretation — useful for writing thesis chapters.

Each notebook is annotated and designed to run end-to-end when the environment and data are available.

---

## Reproducibility / Getting started

Prerequisites
- Conda (recommended) or Python 3.9+ and pip
- ~10–50 GB free disk space (depending on included satellite data)
- Internet connection for downloading external datasets (if raw data is not present in data/)

Create environment (conda)
```bash
conda env create -f environment.yml
conda activate uhi-barcelona
```

Or with pip
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run notebooks
- Start JupyterLab or Jupyter Notebook:
```bash
jupyter lab
# or
jupyter notebook
```
- Open and run notebooks in the order listed above. For non-interactive reproduction you can convert notebooks to scripts or HTML:
```bash
# execute a notebook and save output (nbconvert)
jupyter nbconvert --to notebook --execute notebooks/uhi_satellite_ndvi_ndbi_lst.ipynb --output executed_uhi_satellite.ipynb
```

Run scripts
- Scripts in `scripts/` are callable from the command line. Typical usage:
```bash
python scripts/process_satellite.py --input data/raw/landsat --output data/processed/
```
(Check individual script docstrings / top-of-file help for exact CLI options.)

Notes:
- Some notebooks expect certain file paths under `data/`. If your data are in a different location, update the path variables at the top of each notebook or set environment variables used by `src/config.py`.

---

## Data sources & licensing

The project uses a mix of:
- Local meteorological station data (observations) — check license / data sharing constraints before redistributing.
- Satellite products (e.g., Landsat, Sentinel or MODIS-derived LST/NDVI) — cite the corresponding data provider (USGS, ESA, NASA) and follow their terms of use.

Large raw datasets are not included in the repository. Where feasible, small sample data and processed summaries are included in `data/processed/` to allow the notebooks to run for demonstration purposes.

---

## Outputs

Final results (figures, maps and tables) are stored under `reports/` by theme:
- `reports/temp/` — station and surface temperature visualizations
- `reports/uhi_climatology/` — climatological UHI summaries and maps
- `reports/satelit_info/` — imagery indices and maps (NDVI, NDBI, LST)
- `reports/synthesis/` — thesis-ready figures and summary tables

Generated publication-ready figures are typically in SVG or PNG format and tables as CSV.

---

## Development tips

- Keep computationally heavy processing (satellite preprocessing) separated in `scripts/` to avoid re-running long steps during interactive exploration.
- Use `src/` for functions reused across multiple notebooks (data loaders, plotting helpers).
- Cache intermediate processing results in `data/processed/` to speed up iterative work.

---

## Citation / How to reference

If you use components of this project in your work, please cite the repository and any underlying datasets used. Example (informal):
```
Espinosa-Morros, R. (2025). UHI-Barcelona — urban heat island analysis combining station and satellite data. GitHub repository: https://github.com/rogerespinosamorros/UHI-Barcelona
```
Also cite individual satellite and station data providers as appropriate.

---

## License

This repository is distributed under the MIT License. See LICENSE file for details. If you want a different license, let me know and I can add it.

---

## Contact

Author: rogerespinosamorros — https://github.com/rogerespinosamorros

If you want me to:
- add a shorter abstract for the thesis front page,
- generate a list of figures and tables automatically,
- or expand the reproducibility instructions (e.g., Dockerfile / binder / GitHub Actions),
tell me which and I will prepare it.



