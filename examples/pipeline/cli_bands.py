#!/usr/bin/env python
"""A command-line interface to run a band structure interpolation pipeline."""

import time
import typer
import yaml
from pathlib import Path
from rich import print as rprint

from aiida import orm
from aiida.cmdline.utils import with_dbenv
from aiida_epw.controllers import EpwPrepWorkChainController, EpwBandsCalculationController

app = typer.Typer(pretty_exceptions_show_locals=False)


@app.command()
@with_dbenv()
def init(
    pipeline_settings: Path,
):
    """Initialise the groups for the pipeline."""
    with pipeline_settings.open("r") as handle:
        settings = yaml.safe_load(handle)

    for group_label in settings['groups'].values():
        _, created = orm.Group.collection.get_or_create(group_label)

        if created:
            rprint(f"[bold yellow]Report:[/] created group with label '{group_label}'")


@app.command()
@with_dbenv()
def run(
    pipeline_settings: Path,
):
    """Run the pipeline."""
    with pipeline_settings.open("r") as handle:
        settings = yaml.safe_load(handle)

    epw_prep_controller = EpwPrepWorkChainController(
        **settings['codes'],
        **settings['epw_prep']
    )
    epw_prep_controller.submit_new_batch()

    epw_bands_controller = EpwBandsCalculationController(
        epw_code=settings['codes']['epw_code'],
        **settings['bands_int']
    )
    epw_bands_controller.submit_new_batch()

    rprint(f'[bold blue]Info:[/] Sleeping for 60 seconds...')
    time.sleep(60)


if __name__ == "__main__":
    app()
