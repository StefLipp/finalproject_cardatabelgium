provider "google" {
  credentials = file("/home/stefl/Documents/Secrets/mage_sparkpipeline/mage_planner_key.json")  # Your local credentials to create the VM
  project     = "carprojectbelgium"
  region      = "us-central1"
}

resource "google_compute_instance" "mageai_instance" {
  name         = "mageai-instance"
  machine_type = "e2-medium"
  zone         = "us-central1-a"

  # Attach a Service Account to the instance with the necessary roles
  service_account {
    email  = "mage-planner@carprojectbelgium.iam.gserviceaccount.com"                  # The service account email
    scopes = [
      "https://www.googleapis.com/auth/cloud-platform"        # Full access to Google Cloud resources
    ]
  }

  tags = ["http-server"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  # Add this startup script
  metadata_startup_script = <<-EOT
    #! /bin/bash
    sudo apt-get update
    sudo apt-get install -y docker.io

    # Start Docker
    sudo systemctl start docker
    sudo systemctl enable docker

    # Use gcloud to configure Docker to pull images from Artifact Registry
    gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

    # Run the Docker container with your image
    sudo docker run --name mage_spark -e SPARK_MASTER_HOST='local' -p 6789:6789 -v /home:/home/src \
      us-central1-docker.pkg.dev/carprojectbelgium/my-new-repo/mageai-customsparkfile:tag1 \
      /app/run_app.sh mage start spark_project
  EOT
}

resource "google_compute_firewall" "default" {
  name    = "allow-http"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["6789"]
  }

  source_ranges = ["MY_PUBLIC_IPV4/32"]
  target_tags   = ["http-server"]
}

