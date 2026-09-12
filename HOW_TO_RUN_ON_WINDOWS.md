# Running AtlasFlow on your Windows laptop — no coding needed

You're going to install one free program (Docker Desktop), download a folder, and type
two commands into a black window. That's it. Everything else — the database, the
compute engine, the website — gets built and started automatically.

Budget about 20-30 minutes, most of which is just waiting for downloads.

---

## Step 1: Check your Windows version

Right-click the Start button → **System**. You need **Windows 10 (version 2004 or
higher)** or **Windows 11**. Almost everyone already has this.

---

## Step 2: Install Docker Desktop

1. Go to **https://www.docker.com/products/docker-desktop/**
2. Click **Download for Windows**
3. Once it downloads, double-click the installer file (`Docker Desktop Installer.exe`)
4. Click through the installer with default settings — when it asks about **WSL 2**,
   leave that checkbox **checked** (it's required, and the installer sets it up for you)
5. It will ask you to **restart your computer** — go ahead and do that
6. After restarting, Docker Desktop should open automatically. If not, click the Docker
   whale icon on your Desktop or search "Docker Desktop" in the Start menu
7. It may ask you to accept a license agreement — accept it
8. Wait until you see **"Docker Desktop is running"** with a green light in the
   bottom-left of the Docker window. This can take a minute or two the first time.

**If Windows shows an error about WSL2 during install:** open the Microsoft Store, search
for **"Windows Subsystem for Linux"**, install it, restart your computer, then reopen
Docker Desktop. This only happens on some older setups.

You can leave Docker Desktop running in the background from now on — it just needs to be
open (you'll see its icon in your system tray, bottom-right of the screen) whenever you
want to use AtlasFlow.

---

## Step 3: Get the AtlasFlow files onto your computer

1. Download the `atlasflow.zip` file (from this chat) to somewhere easy to find, like
   your **Desktop** or **Downloads** folder
2. Right-click the zip file → **Extract All...** → choose a location (Desktop is fine) →
   **Extract**
3. You should now have a folder called `atlasflow` with files like `README.md`,
   `docker-compose.yml`, etc. inside it. That confirms it extracted correctly.

---

## Step 4: Open a terminal in that folder

A "terminal" is just a window where you type commands instead of clicking. Here's the
easiest way to open one already pointed at the right folder:

1. Open the `atlasflow` folder in **File Explorer** (double-click it)
2. Click into the address bar at the top of the File Explorer window (where it shows the
   folder path) — click once so it highlights
3. Type `powershell` and press **Enter**

A blue/black window will pop up. This is PowerShell, and it's already "in" your
`atlasflow` folder — you don't need to type any `cd` commands.

---

## Step 5: Start AtlasFlow

In that PowerShell window, type this exactly and press **Enter**:

```
docker compose up --build
```

Now wait. The first time you run this, it downloads and builds everything — this can
take **10-20 minutes** depending on your internet speed, and it's normal. You'll see a
lot of text scrolling by; that's expected and not an error.

You'll know it's ready when the scrolling slows down and you start seeing lines that look
like:
```
backend-1    | INFO:     Application startup complete.
frontend-1   | ...
```

**Leave this PowerShell window open** — closing it stops AtlasFlow. You can minimize it.

---

## Step 6: Open AtlasFlow in your browser

Open Chrome, Edge, or any browser and go to:

```
http://localhost:3000
```

You should see the AtlasFlow sign-in screen.

**Log in with:**
- Username: `admin`
- Password: `atlasflow-admin`

---

## Step 7: Change the default password (do this now)

That default password is meant to be temporary. Once logged in:

1. Click the **Users** tab in the sidebar
2. Create a new user for yourself with your own username/password and role `admin`
3. Sign out, sign back in as your new user
4. (Optional, more advanced) You can retire the `admin` account later once you're
   comfortable — not required to get started.

---

## Everyday use after today

**To use AtlasFlow again later:**
1. Make sure Docker Desktop is open (check the system tray)
2. Open PowerShell in the `atlasflow` folder (Step 4 above)
3. Type `docker compose up` (no `--build` needed this time — that part's already done) and press Enter
4. Go to `http://localhost:3000`

**To stop AtlasFlow:**
- Go to the PowerShell window and press `Ctrl + C`, or just close the window

**To fully shut it down and free up computer resources:**
```
docker compose down
```
(type this in the PowerShell window, in the `atlasflow` folder)

---

## If something doesn't work

| What you see | What to do |
|---|---|
| "docker compose" says it's not a recognized command | Docker Desktop isn't running — open it from the Start menu and wait for the green "running" light, then try again |
| A port/address already in use error | Something else on your computer is using that address — try restarting your computer and running `docker compose up` again |
| The browser says "can't connect" at localhost:3000 | Give it another minute — check the PowerShell window for a line saying the frontend started, then refresh |
| Everything seems stuck for a very long time on first run | Normal on slower internet — it's downloading several gigabytes total. Let it keep going. |

If you get stuck, copy the error message you see in the PowerShell window and share it —
that's usually enough to figure out exactly what's wrong.
