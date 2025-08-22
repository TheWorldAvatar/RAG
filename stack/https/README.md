## HTTPS setup

These instructions are adapted from <https://mindsers.blog/en/post/https-using-nginx-certbot-docker/>.

1) The committed [nginx\conf\default.conf](nginx\conf\default.conf) is the final state. It is necessary to modify the file during the initial setup. Initially, it should only contain the definition of the server that listens on port 80. Then, spin up nginx, i.e.:

    ```bash
    docker compose up webserver -d
    ```

    Make sure the domain name (i.e. the `server_name`) maps to the IP address of the machine. Note that certbot must be able to contact the IP address on port 80 while creating the certificate.

2) Execute the following (dry run):

    ```bash
    docker compose run --rm  certbot certonly --webroot --webroot-path /var/www/certbot/ --dry-run -d qa.theworldavatar.io
    ```

3) If successful, rerun certbot without `--dry-run`:

    ```bash
    docker compose run --rm  certbot certonly --webroot --webroot-path /var/www/certbot/ -d qa.theworldavatar.io
    ```

4) Revert the changes in [nginx\conf\default.conf](nginx\conf\default.conf). Adjust the IP address in the proxy pass, if necessary.

5) Restart nginx:

    ```bash
    docker compose restart webserver
    ```

6) Setup should be complete at this stage. The certificate needs to be renewed every three months with the following command:

    ```bash
    docker compose run --rm certbot renew
    ```

