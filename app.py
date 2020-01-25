import argparse

from core.etl import etl


def main(args):

    etl.run(dry_run=args.dry_run)


if __name__ == '__main__':
    """
    Enables command line parameters to be passed to the application to choose
    the execution mode.

    Args:
        --dry_run (flag): From the terminal, start the application with this
        flag to teardown the AWS infrastructure on completion.
        Example: python app.py --dry_run

        --live (flag): From the terminal, start the application with this flag
        to retain the the AWS infrastructure on completion.
        Example: python app.py --live
    """

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--dry_run',
        dest='dry_run',
        action='store_true',
        help='Teardown AWS infrastructure after ETL operation.',
        )
    parser.add_argument(
        '--live',
        dest='dry_run',
        action='store_false',
        help='Retain AWS infrastructure after ETL operation.',
    )
    parser.set_defaults(dry_run=True)

    args = parser.parse_args()

    main(args)
