import argparse

from core.etl import etl


def main(args):

    etl.run(dry_run=args.dry_run)


if __name__ == '__main__':
    """
    Enables command line parameters to be passed to the application to determine
    the execution mode.

    Args:
        --dry_run (flag): From the terminal, start the application with this flag
        to teardown the Redshift cluster and AWS infrastructure upon completion.
        Example: python app.py --dry_run

        --live (flag): From the terminal, start the application with this flag to
        retain the Redshift cluster and AWS infrastructure upon completion.
        Example: python app.py --live
    """

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--dry_run',
        dest='dry_run',
        action='store_true',
        help='Teardown Redshift cluster and AWS infrastructure after ETL operation.',
        )
    parser.add_argument(
        '--live',
        dest='dry_run',
        action='store_false',
        help='Retain Redshift cluster and AWS infrastructure after ETL operation.',
    )
    parser.set_defaults(dry_run=True)

    args = parser.parse_args()

    main(args)
