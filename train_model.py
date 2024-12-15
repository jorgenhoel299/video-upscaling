import os
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms
from PIL import Image
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.rdd import RDD

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


# Define SRGAN Model
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


def distributed_training(data_dirs):
    """
    Train an SRGAN model on the local data available on a worker node.
    """
    highres_dir, lowres_dir = data_dirs

    print(f"Worker processing: High-res: {highres_dir}, Low-res: {lowres_dir}")

    if not os.path.exists(highres_dir) or not os.path.exists(lowres_dir):
        print(f"Directories not found: {highres_dir}, {lowres_dir}")
        return []

    transform = transforms.Compose([
        transforms.ToTensor()
    ])

    dataset = SuperResolutionDataset(highres_dir, lowres_dir, transform)
    if len(dataset) == 0:
        print(f"No data found in: High-res: {highres_dir}, Low-res: {lowres_dir}")
        return []

    print(f"Dataset size: {len(dataset)}")

    data_loader = DataLoader(dataset, batch_size=16, shuffle=True)
    model = SRGAN().to(device)
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    trained_model = train_model(data_loader, model, criterion, optimizer, epochs=5)

    local_model_path = f"/opt/spark/models/model_{os.path.basename(highres_dir)}.pth"
    torch.save(trained_model.state_dict(), local_model_path)
    print(f"Model saved at: {local_model_path}")

    return [local_model_path] if os.path.exists(local_model_path) else []


def discover_local_datasets(base_path="/opt/spark/data/training/frames/"):
    """
    Discover datasets available on this container (master or worker).
    """
    video_dirs = [os.path.join(base_path, d) for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
    highres_dirs = [d for d in video_dirs if not d.endswith("_lowres")]
    lowres_dirs = [f"{d}_lowres" for d in highres_dirs if os.path.exists(f"{d}_lowres")]

    pairs = [(highres, lowres) for highres, lowres in zip(highres_dirs, lowres_dirs)]
    return pairs


def main():
    # Initialize Spark
    spark = SparkSession.builder.appName("DistributedSRGANTraining").getOrCreate()
    sc = SparkContext.getOrCreate()

    # Run discovery on local datasets (master + workers)
    print("Discovering datasets on local containers...")
    local_datasets = sc.parallelize([None], 1).mapPartitions(lambda _: discover_local_datasets()).collect()

    # Flatten list of discovered datasets
    datasets = [pair for sublist in local_datasets for pair in sublist]
    print(f"Discovered datasets: {datasets}")

    if not datasets:
        print("No datasets found. Exiting...")
        spark.stop()
        return

    # Distribute the discovered datasets for training
    model_paths = sc.parallelize(datasets, len(datasets)).map(distributed_training).collect()

    print(f"Intermediate models saved: {model_paths}")

    # Stop Spark
    spark.stop()


if __name__ == "__main__":
    main()
