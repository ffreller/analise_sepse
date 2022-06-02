import os
import sys
import argparse


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="My parser")
    parser.add_argument('--teste', dest='test', action='store_true')
    parser.set_defaults(test=False)
    args = parser.parse_args()
    test = args.test
    
    # Set library path for SQLAlchemy
    this_dir = os.path.dirname(os.path.realpath(__file__))
    instant_client_path = os.path.join(this_dir, 'instantclient_21_5')
    # Oracle client path
    os.environ['LD_LIBRARY_PATH'] = instant_client_path
    os.environ['TZ'] = 'America/Sao_Paulo'
    if test:
        # Run program.py in test version
        os.execv(sys.executable, [sys.executable] + [this_dir+'/program.py'] + ['--test'])
    else:
        # Run program.py in production version
        os.execv(sys.executable, [sys.executable] + [this_dir+'/program.py'])

