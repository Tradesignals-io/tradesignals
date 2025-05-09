# user.py

import click


@click.command()
@click.option("--name", prompt="Username")
@click.option("--password", prompt="Password", hide_input=True)
def cli(name, password):
    if click.confirm("Would you like to create a pipeline?"):
        click.echo("Pipeline successfully created!")
    else:
        click.echo("You must create a pipeline to continue!")
    if name != read_username() or password != read_password():
        click.echo("Invalid user credentials")
    else:
        click.echo(f"User {name} successfully logged in!")


def read_password():
    return "secret"


def read_username():
    return "admin"


if __name__ == "__main__":
    cli()
