import os
import psutil
import sys

# Cross-platform lock file path (Windows + Linux)
LOCK_FILE = os.path.join(os.path.expanduser("~"), ".can_assure_app.pid")

def check_already_running():
    """Prevent launching a second instance of the application."""
    try:
        if os.path.exists(LOCK_FILE):
            with open(LOCK_FILE, "r") as f:
                old_pid = int(f.read().strip())

            # Check if that PID is alive
            if psutil.pid_exists(old_pid):
                print("\n❌ Application is already running!")
                print(f"⚠️ Running Instance PID: {old_pid}")
                print("➡️ Close the running application and try again.\n")
                sys.exit(1)

        # Not running → store new PID
        with open(LOCK_FILE, "w") as f:
            f.write(str(os.getpid()))

    except Exception as e:
        print(f"⚠️ Lock check failed: {e}")
        # Fallback: allow app to run, but remove corrupted lock
        try:
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
        except:
            pass

# --- RUN CHECK BEFORE IMPORTING GUI/BACKEND ---
check_already_running()

#
import gui


def main():
    gui.main(gui.root)


if __name__ == "__main__":
    main()
