import xarray as xr
import fsspec
import numpy as np
import s3fs
import zarr
from tqdm import tqdm
from dask.distributed import Client

def main():
    base_url = 's3://noaa-nws-aorc-v1-1-1km'
    #start_date = "2019-01-01" 
    #end_date = "2025-04-30"  
    client = Client()
    for year_ in tqdm(range(2019, 2026)):
        year = str(year_)
        print(f"Processing year: {year}")
        year_url = f'{base_url}/{year}.zarr/'        
        ds = xr.open_zarr(fsspec.get_mapper(single_year_url, anon=True), consolidated=True)
        if start_date or end_date:
            ds = ds.sel(time=slice(start_date, end_date))
        ds_single = ds_single.sortby("time")
        daily_groups = ds_single.resample(time="24H")
        for i, (day, daily_data) in enumerate(daily_groups):
            date_str = np.datetime_as_string(day, unit='D').replace('-', '')
            filename = f'noaa_aorc_usa_{year}_day_{date_str}.nc'
            daily_data.to_netcdf(filename)
            print(f"Saved {filename}")

if __name__ == "__main__":
    main()
