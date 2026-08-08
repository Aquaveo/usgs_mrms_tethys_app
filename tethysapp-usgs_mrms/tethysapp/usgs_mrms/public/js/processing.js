const BASE_URL = "/apps/usgs-mrms/";

const PROCESS_URLS = {
    'basin_download': BASE_URL + "do_download_basin/",
    'zarr_download': BASE_URL + "do_download_zarr/",
};

const DOWNLOAD_PROCESS_BASE_URL = BASE_URL + "basin/";
const FLOOD_ALERT_PROCESS_BASE_URL = BASE_URL + "flood-alert/";

let csrfToken;
let state;
let gageId;
let runId;
let workers;
let processType;
let jobId;
let jobStatus;

function setMessage(message) {
    const el = document.querySelector(".process-container p");
    if (el && message) el.textContent = message;
}

// --- synchronous downloads (basin geojson / zarr): unchanged behavior ---
async function runDownload() {
    let url;
    if (processType === "basin_download") {
        url = PROCESS_URLS.basin_download + state + "/";
    } else if (processType === "zarr_download") {
        url = PROCESS_URLS.zarr_download + state + "/" + gageId + "/";
    }
    try {
        const res = await fetch(url, { method: "POST", headers: { "X-CSRFToken": csrfToken } });
        const data = await res.json();
        if (data.status === "success") {
            if (processType === "basin_download") {
                window.location.href = DOWNLOAD_PROCESS_BASE_URL + state + "/";
            } else {
                window.location.href = DOWNLOAD_PROCESS_BASE_URL + state + "/" + gageId + "/";
            }
            return;
        }
        if (res.status === 404 && processType === "basin_download") {
            showError('No basin data could be found for the specified state.');
        } else if (res.status === 404 && processType === "zarr_download") {
            showError('No data could be found for the specified gage ID. Try again later, as this data may not yet be available in the system.');
        } else {
            showError("Download failed");
        }
    } catch (err) {
        showError("Download failed");
        console.error(err);
    }
}

// --- flood alert: poll the background job, then load results ---
function goToResults() {
    window.location.href = FLOOD_ALERT_PROCESS_BASE_URL + "results/" + state + "/" + runId + "/";
}

async function pollFloodAlert() {
    // Server told us the run already exists, or there is no job to poll.
    if (jobStatus === "success" || !jobId) {
        goToResults();
        return;
    }
    try {
        const res = await fetch(FLOOD_ALERT_PROCESS_BASE_URL + "status/" + jobId + "/");
        const data = await res.json();
        if (data.status !== "success" || !data.job) {
            showError("Flood alert generation failed.");
            return;
        }
        const job = data.job;
        setMessage(job.message || "Generating flood alert results…");
        if (job.job_status === "success") {
            goToResults();
        } else if (job.job_status === "error" || job.job_status === "interrupted") {
            showError(job.message || "Flood alert generation failed.");
        } else {
            setTimeout(pollFloodAlert, 3000);
        }
    } catch (err) {
        showError("Flood alert generation failed.");
        console.error(err);
    }
}

function runProcess() {
    if (processType === "flood_alert") {
        pollFloodAlert();
    } else {
        runDownload();
    }
}

function loadProcessData() {
    const processData = JSON.parse(
        document.getElementById("processing-data").textContent
    );
    csrfToken = processData.csrfToken;
    state = processData.state;
    gageId = processData.gageId;
    runId = processData.runId;
    workers = processData.workers;
    processType = processData.processType;
    jobId = processData.jobId;
    jobStatus = processData.jobStatus;
}

function showError(message) {
    document.querySelector(".process-container").style.display = "none";
    document.querySelector(".error-message-container").style.display = "block";
    document.querySelector(".error-message").textContent = message;
}

function returnToPreviousPage() {
    if (processType === "basin_download") {
        window.location.href = DOWNLOAD_PROCESS_BASE_URL;
    } else if (processType === "zarr_download") {
        window.location.href = DOWNLOAD_PROCESS_BASE_URL + state + "/";
    } else {
        window.location.href = FLOOD_ALERT_PROCESS_BASE_URL;
    }
}

document.addEventListener("DOMContentLoaded", () => {
    loadProcessData();
    runProcess();
});
