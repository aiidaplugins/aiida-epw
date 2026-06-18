# Pipeline example

In this directory you can find an example of what we call a "pipeline", i.e. a collection of submission controllers that run different processes from a "parent group".
This basic example runs two processes:

1. The `EpwPrepWorkChain`, based on a group of initial structures.
2. A `EpwCalculation` for interpolating the band structure, based on the `EpwPrepWorkChain` and the restart files it produces.

>[!WARNING]
> The `aiida-submission-controller` currently still uses the `has_key` filter, which means it can only be used with a PostgreSQL database, see:
> 
> https://github.com/aiidateam/aiida-submission-controller/issues/28

## Steps

The "pipeline" is basically a CLI wrapped around the submission controllers.
I typically use `typer` for my CLI apps, so you have to install that in your environment:

```
pip install typer 
```

To make the pipeline CLI easier to run, make it executable:

```
chmod +x cli_bands.py
```

The `cli_bands_settings.yaml` file contains all the "pipeline settings", i.e.:

- The AiiDA codes required to run the processes.
- The groups used in the pipeline to store the structures/processes.
- More detailed inputs for each of the processes.

Adapt the inputs as needed.
You can then create the groups used by the submission controllers with:

```
./cli_bands.py init cli_bands_settings.yaml
```

and run the pipeline via:

```
./cli_bands.py init cli_bands_settings.yaml
```
