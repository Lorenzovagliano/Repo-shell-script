#### To run this script in the SCOAP3 repo. Follow these steps: 
- Access scoap3-backend-prod on Argocd.
- Access one of the two scoap3-backend-web pods.
- Access the terminal.
- Run `python manage.py shell`
- Paste the script and wait for it to run.
- Leave the python shell via `exit`
- Upload the `out.csv` file to cernbox using curl via [this method](https://cernbox.docs.cern.ch/for_developers/api_access/).
- For the current cernbox folder we're using, for example, you can do: `curl -T out.csv -X PUT https://cernbox.cern.ch/remote.php/dav/public-files/3YZE56ZmXPHaeSd/test.csv`

You may alter the parameters in line 405:
`result = year_export("2024-10-01", "2024-12-31", "Elsevier")`
