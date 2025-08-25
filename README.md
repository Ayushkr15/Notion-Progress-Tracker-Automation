# Notion Progress Tracker Automation

This project provides a powerful, automated system to link your daily tasks in Notion to their corresponding weekly and monthly progress reports. Set it up once, and a secure script will run automatically to keep your workspace perfectly organized.

This system is designed to be used with the [Command Center Productivity System](https://ayushkr15.gumroad.com/l/GTD) Notion template.

## ✨ Features
- Fully Automated: A script runs every hour to automatically link new tasks.
- On-Demand Sync: Manually trigger the sync anytime you want directly from GitHub.
- Secure: Your Notion keys are stored safely using encrypted GitHub Secrets.
- Easy Setup: No coding knowledge required. Just follow the step-by-step guide.
- Free to Run: Uses free tiers of Notion and GitHub Actions.

## 🚀 How It Works
This system uses a Python script hosted in your private GitHub repository. A GitHub Actions workflow runs this script on a schedule (every hour). The script communicates with the Notion API to find tasks that you've recently added or modified and automatically creates the relation links to your Weekly Progress and Monthly Progress databases.

---

## ⚙️ Quick Start Guide
- Get the Notion Template: Duplicate the required Notion pages from the Gumroad product.
- Create Repository: Use this repository as a template to create your own private copy.
- Set Up Notion: Create a Notion Integration and get your API Key and Database IDs.
- Configure GitHub: Add your Notion keys as encrypted secrets in your repository settings.
- Activate: Run the automation for the first time from the "Actions" tab.
