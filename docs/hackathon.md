# Hackathon 2025

Met Office, Exeter, UK, 10 - 14 May 2025

Hybrid hackathon, being run by the Model Benchmarking Task Team and the REF delivery team. We welcome attendance from technical and domain scientists from modelling centres involved in the CMIP AR7 Fast Track, observation dataset providers as well as ESGF nodes and developers.

During the hackathon there will also be dedicated drop-ins for wider community interest for:

* **10 March 14:00 UTC – 15:00 UTC** Hackathon launch -providing a brief overview of the REF and a status update by the AR7 FT REF delivery team
* **11 March 17:00 – 18:00 UTC** Modelling Centres
* **13 March 08:00 – 09:00 UTC** Modelling Centres
* **13 March 11:00 – Midday UTC** Observation dataset providers

## What can you do before the hackathon?

Before attending the hackathon, it would be useful to clone the package and setup your local environment by following the [development Installation](development.md#development-installation) instructions.

It would also be useful to clone the [sample data repository](https://github.com/Climate-REF/ref-sample-data).
Depending on your area of interest,
you may wish to add additional sample data to the test suite.

After installing the database,
you can run the test suite using `make test` to ensure that everything is working as expected.
This will fetch the sample data and run the tests.

If there are any issues with the installation, please raise an issue in the [issue tracker](https://github.com/Climate-REF/climate-ref/issues) so that we can help you get set up.

### Additional data

We welcome the testing of ingesting additional local datasets into the REF.
We currently support ingesting CMIP6-like and obs4MIPs datasets,
but are interested in hearing about other datasets that you would like to see supported.

We have tested a subset of CMIP6 data,

The metrics that are currently available in the REF are relatively limited,


### Reading material

For those interested in learning more about the REF,
we recommend reading the
[Architecture design document](background/architecture.md).
This outlines the design of the REF and provides some backaground about the project.


## Finally

We are working towards a beta release of the REF in the coming months so this project is under heavy development so there will be things that don't work as expected.
Please raise [issues](https://github.com/Climate-REF/climate-ref/issues) if anything doesn't work as expected or if you have any behaviour that you would like to see implemented.
Please do get involved and help us to shape the future of the REF.
