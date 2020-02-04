# Installing `bbconf`

Install from [GitHub releases](https://github.com/databio/bbconf/releases) or from PyPI using `pip`:

- `pip install --user bbconf`: install into user space.
- `pip install --user --upgrade bbconf`: update in user space.
- `pip install bbconf`: install into an active virtual environment.
- `pip install --upgrade bbconf`: update in virtual environment.

See if your install worked by calling `bbconf -h` on the command line. If the `bbconf` executable in not in your `$PATH`, append this to your `.bashrc` or `.profile` (or `.bash_profile` on macOS):

```{console}
export PATH=~/.local/bin:$PATH
```