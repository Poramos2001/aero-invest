# aero-invest

Link to the ASN [Database standards](https://asn.flightsafety.org/about/ASN-standards.pdf)

To set up **ChromeDriver** on Ubuntu, follow these steps:


### 🔍 Why you need Google Chrome to use ChromeDriver

**Yes**, you do need to have **Google Chrome installed** on your system to use **ChromeDriver** effectively. Here's why:

---

### ✅ ChromeDriver is tightly coupled with Chrome

- **ChromeDriver is a bridge** between Selenium and the Chrome browser.
- It **controls a real Chrome browser instance** (even in headless mode).
- It must **match the version** of Chrome installed on your system to work properly.

If Chrome is missing, ChromeDriver won’t have a browser to launch, and you’ll get errors like:

```
selenium.common.exceptions.WebDriverException: Message: unknown error: cannot find Chrome binary
```

---


### ✅ **Step 1: Install Google Chrome (if not already installed)**

If you don’t have Chrome yet:

```bash
sudo apt update
sudo apt install wget
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb
```