## How to run the program

In this example we will compare multiple buckets of three different AWS accounts.

Update the `s3-uris-to-analyze.txt` file with your data.

In your terminal, generate AWS credentials to allow botocore to connect with your AWS account.

Run:

```bash
python extract.py
```

Now we have the results for the buckets of one account, to work with another account we must execute the following manual steps to save these results:

```bash
mkdir exports-all-aws-accounts
mv exports exports-all-aws-accounts/aws-account-1
```

As you can see, now the `exports` folder does not exists, this is required to generate new results for the other AWS accounts.

Let's create the second AWS account results!

After update the `s3-uris-to-analyze.txt` file with the second account S3 URIs, we authenticate in the terminal to the second AWS account and run:

```bash
python extract.py
```

We save the created files:

```bash
mv exports exports-all-aws-accounts/aws-account-2
```

We repeat the steps for the third account:

1. Update the `s3-uris-to-analyze.txt` file with the account S3 URIs to analyze.
2. Authenticate in the terminal to the second AWS account.
3. Generate the URIs results running: `python extract.py`
4. Save the created files: `mv exports exports-all-aws-accounts/aws-account-3`
