# Roadmap

Below is a high-level roadmap for the CMIP7 Assessment Fast Track(AFT)  project towards the goal of a public release in October 2025.
This roadmap outlines the key milestones and tasks that need to be completed to achieve this goal.

We have broken down the roadmap into several sections to provide a clear overview of the project's progress.
Namely,

- Diagnostics
- Infrastructure
- Visualisation
- Testing
- Deployment
- Community Engagement

The roadmap is subject to change as the project progresses, and adjustments may be made based on new insights, feedback, and evolving project requirements.

/// admonition | Note

Click on [] in the top right corner of the diagram to make it full screen.
///

```mermaid
gantt
    dateFormat  YYYY-MM-DD
    title       REF for CMIP7 Assessment Fast Track
    excludes    weekends

    section Diagnostics

    Develop list of diagnostics                   :done, metrics, 2024-10-06, 30d
    Prototype metric package                      :done, metricsPrototype, 2024-10-06, 30d

    Create ESMValTool integration                 :done, esmvaltool, after metricsPrototype, 2025-01-06
    Create ILAMB integration                      :done, ilamb, after metricsPrototype, 2025-01-20
    Create PMP integration                        :done, pmp, after metricsPrototype, 2025-03-04

    Implement missing diagnostics                 :done, missing, after esmvaltool, 2025-05-14
    Incorporate missing reference datasets        :done, after beta, 4w
    Fixes                                         :active, after missing, 14w

    section Infrastructure

    Architecture design                           :done, 2024-10-18, 20d
    Ingest CMIP6 datasets                         :done, cmip6, 2024-11-01, 20d
    Ingest obs4MIPs datasets                      :done, 2025-01-20, 2025-03-01
    Develop a local executor                      :done, local, after cmip6, 20d
    Core docker container                         :done, dockerise, 2025-01-10, 10d
    Conda-forge environments                      :conda, 2025-04-05, 3w
    Docker containers                             :done,after conda, 3w
    Develop a remote executor                     :done,celery, 2025-01-01, 30d
    Ingest scalar results into database           :done,ingest, after results, 30d
    Ingest series results into database           :done,ingest-series, after ingest, 30d
    Develop a remote executor (slurm)             :done, slurm, 2025-04-20, 10d
    Ingest CMIP7 AFT datasets                        :2025-12-01, 2w

    section Visualisation

    CMEC validation                               :done, cmec, 2025-01-15, 20d
    CMEC helpers                                  :done, cmecHelpers, after cmec, 4w
    Basic API for results                         :done, results, 2025-03-01, 20d
%%    Integrate Unified Dashboard                   :active, ud, after results, 20d
    Search results via API                        :done,api-result, after beta, 2w
    Example prototype dashboard                   :done,dashboard, after api-result, 6w
    Refine dashboard                              :active,after dashboard, 6w
    Python package for consuming results          :after public, 4w


    section Testing
    Validate package licences                     :done, 2024-11-15, 2024-12-02
    Develop testing framework                     :done, 2024-11-10, 2024-12-10
    Documentation and tutorials                   :done, 2025-05-06, 2025-04-01
    Initial modelling center testing (MetOffice)  :done, mc1, 2025-03-10, 14d
    Initial modelling center testing (MC 2)       :done, mc2, 2025-05-10, 14d
    Initial modelling center testing (MC 3)       :done, mc3, 2025-05-10, 14d
    MB TT stress-testing                          :done, 2025-05-10, 20d
    MB TT Documentation Review                    :done, 2025-05-20, 20d

    section Deployment
    Discuss options with ESGF deployment          :done, esgf, 2024-12-01, 90d
    Build K8 helm charts                          :crit, active, helm, 2025-08-01, 4w
    ESGF Staging deployment for testing           :crit, staging, after helm, 1w
    CMIP6 stress-test                             :after staging, 4w
    ESGF index integration                        :esgf-index, after public, 4w
    ESGF Production deployment                    :prod, after esgf-index, 1w
    ESGF Data Challenge                           :after prod, 30d

    section Community Engagement
    Model Evaluation Survey                       :done, 2024-05-01, 4w
    Modelling Centre Survey                       :done, 2024-06-01, 4w
    AOGS                                          :done, 2024-06-25, 5d
    Diagnostics Survey                            :done, survey, 2024-10-01, 4w
    ESA CMUG Colocation                           :done, 2024-10-16, 3d
    Project Launch                                :done, 2024-11-04, 1d
    AGU                                           :done, 2024-12-01, 1w
    Hackathon                                     :done, hackathon, 2025-03-10, 5d
    EGU                                           :done, egu, 2025-04-20, 1w
    Beta feature freeze                           :done, milestone, beta, 2025-05-01, 1d
    Beta Release                                  :done, milestone, beta, 2025-05-30, 1d
    Drop-ins/Engagement                           :done, engagement, after beta, 2w
    Incorporate feedback                          :active, after engagement, 12w
    ESA Living Planet Symposium                   :done, lps, 2025-06-23, 1w
    Public Release                                :milestone, public, 2025-10-30, 1d

```

## Diagnostics

An overview of the current state of the selected diagnostics can be found in the
[Diagnostics Github board](https://github.com/orgs/Climate-REF/projects/2/views/2) document.
This is being updated as the new metrics are integrated into the CMIP7 AFT REF.


## What will be in the public v1 release:

We have a public release planned for October 2025 during the ESM2025 General Assembly.
This will mark version 1.0 of the CMIP7 AFT REF and be hosted by the Earth System Grid Federation (ESGF).

This release will include diagnostic outputs from ESMValTool, PMP and ILAMB3 calculated from CMIP6 and CMIP6plus
datasets. Users will also be able to host their own instance of the REF to run diagnostics on datasets not available
on ESGF.

The release will include the following features:

* Ingesting local CMIP6, CMIP7 AFT, obs4MIPs datasets
    * We will include documentation to allow users to ingest their own datasets (we welcome any contributions)
* ESMValTool, ILAMB, and PMP metrics diagnostics
* A public web interface to view the results of any metrics executions
* A portrait plot contains results from the 3 different providers
* The ability to run metrics locally, using docker containers (via celery) and via Slurm.
* Documentation and tutorials

### What is not planned to be in the beta

* CMIP7 AFT datasets - this is waiting on datasets being available
* Integration with the ESGF indexes - that work is planned for after the release
* A python API to consume the results easily (results can be downloaded from the web interface)
* conda-forge packages for the metrics providers
