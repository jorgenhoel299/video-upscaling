import os
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms
from PIL import Image
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

# Set device to GPU if available, else CPU
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Define Dataset class for loading frames
class SuperResolutionDataset(Dataset):
    def __init__(self, highres_dir, lowres_dir, transform=None):
        self.highres_dir = highres_dir
        self.lowres_dir = lowres_dir
        self.transform = transform
        self.highres_images = sorted(os.listdir(highres_dir))
        self.lowres_images = sorted(os.listdir(lowres_dir))

    def __len__(self):
        return len(self.highres_images)

    def __getitem__(self, idx):
        highres_path = os.path.join(self.highres_dir, self.highres_images[idx])
        lowres_path = os.path.join(self.lowres_dir, self.lowres_images[idx])

        highres_img = Image.open(highres_path).convert("RGB")
        lowres_img = Image.open(lowres_path).convert("RGB")

        if self.transform:
            highres_img = self.transform(highres_img)
            lowres_img = self.transform(lowres_img)

        return lowres_img, highres_img


# Define SRGAN Model (simplified for example purposes)
class SRGAN(nn.Module):
    def __init__(self):
        super(SRGAN, self).__init__()
        self.upscale = nn.Sequential(
            nn.Conv2d(3, 64, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.Conv2d(64, 3, kernel_size=3, stride=1, padding=1)
        )

    def forward(self, x):
        return self.upscale(x)


# Training function
def train_model(data_loader, model, criterion, optimizer, epochs=5):
    model.train()
    for epoch in range(epochs):
        total_loss = 0
        for lowres, highres in data_loader:
            lowres = lowres.to(device)
            highres = highres.to(device)

            optimizer.zero_grad()
            outputs = model(lowres)
            loss = criterion(outputs, highres)
            loss.backward()
            optimizer.step()

            total_loss += loss.item()

        print(f"Epoch {epoch + 1}/{epochs}, Loss: {total_loss / len(data_loader):.4f}")
    return model


# Master processing function
def process_master_data(master_pairs):
    print(f"Master processing {len(master_pairs)} pairs of directories...")
    model_paths = []
    for highres_dir, lowres_dir in master_pairs:
        print(f"Master processing pair: High-res: {highres_dir}, Low-res: {lowres_dir}")

        if not os.path.exists(highres_dir) or not os.path.exists(lowres_dir):
            print(f"Directories not found: High-res: {highres_dir}, Low-res: {lowres_dir}")
            continue

        transform = transforms.Compose([
            transforms.Resize((128, 128)),
            transforms.ToTensor()
        ])

        dataset = SuperResolutionDataset(highres_dir, lowres_dir, transform)
        print(f"Dataset size for High-res: {highres_dir}: {len(dataset)}")

        if len(dataset) == 0:
            print(f"No data found in: High-res: {highres_dir}, Low-res: {lowres_dir}")
            continue

        data_loader = DataLoader(dataset, batch_size=16, shuffle=True)
        model = SRGAN().to(device)
        criterion = nn.MSELoss()
        optimizer = optim.Adam(model.parameters(), lr=0.001)

        try:
            trained_model = train_model(data_loader, model, criterion, optimizer, epochs=5)
        except Exception as e:
            print(f"Training failed for High-res: {highres_dir}, Low-res: {lowres_dir}. Error: {e}")
            continue

        # Ensure model directory exists
        model_dir = "/opt/spark/models/"
        os.makedirs(model_dir, exist_ok=True)

        local_model_path = f"{model_dir}/model_{os.path.basename(highres_dir)}.pth"
        try:
            torch.save(trained_model.state_dict(), local_model_path)
            print(f"Master saved model: {local_model_path}")
            model_paths.append(local_model_path)
        except Exception as e:
            print(f"Failed to save model for High-res: {highres_dir}. Error: {e}")

    return model_paths


# Worker processing function
def distributed_training(partition_data):
    """
    Worker processes its partition of data.
    """
    pairs = list(partition_data)
    print(f"Worker processing {len(pairs)} pairs")
    return process_master_data(pairs)  # Reuse the logic for master but applied on worker-specific data.


def main():
    # Initialize Spark
    spark = SparkSession.builder.appName("DistributedSRGANTraining").getOrCreate()
    sc = SparkContext.getOrCreate()

    # List all video directories and corresponding low-resolution directories
    base_dir = "/opt/spark/data/training/frames/"
    video_dirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    highres_dirs = [d for d in video_dirs if not d.endswith("_lowres")]
    lowres_dirs = [f"{d}_lowres" for d in highres_dirs]

    # Ensure all low-res directories exist
    pairs = [(highres, lowres) for highres, lowres in zip(highres_dirs, lowres_dirs) if os.path.exists(lowres)]

    # Split data into master-local and worker-local partitions
    master_pairs = [pair for pair in pairs if os.path.exists(pair[0])]  # Filter master-local paths
    worker_pairs = [pair for pair in pairs if pair not in master_pairs]  # Remaining pairs for workers

    # Process master's local data
    master_model_paths = process_master_data(master_pairs)

    # Parallelize video directories across all workers
    worker_model_paths = sc.parallelize(worker_pairs, len(worker_pairs)).mapPartitions(distributed_training).collect()

    # Combine all intermediate model paths
    all_model_paths = master_model_paths + [path for sublist in worker_model_paths for path in sublist]
    print(f"Intermediate models saved: {all_model_paths}")

    # Stop Spark
    spark.stop()


if __name__ == "__main__":
    main()
