import sys


def checker_command(checker, target, *flags):
    command = [sys.executable, "-m", checker]
    if checker == "pyright":
        command.extend(["--pythonpath", sys.executable])
    return [*command, *flags, str(target)]
