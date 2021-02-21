# portfolio-analyzer
Provides an insight into stock portfolios

Currently, the supported input formats are the following files saved via ATrad online trading platform.
  * "Account" file - The full account statement; Exported via Client -> Account Statement -> Set date range to cover the entire trade history -> Save
  * "Portfolio.xlsx" file (optional) - Exported via Client -> Portfolio -> Save

These files should be placed in the current working directory where the script is executed from.



## Setup

### From pip

    $ pip install --user -r requirements.txt


# Running

    $ ./pf-analyzer.py
