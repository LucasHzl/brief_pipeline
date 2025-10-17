from pathlib import Path
import requests
from datetime import datetime

class NYCTaxiDataDownloader:
    def __init__(self, base_url, year, data_dir, base_file_name):
        self.base_url = base_url
        self.year = year
        self.data_dir = Path(data_dir)
        self.base_file_name = base_file_name

        # Crée le dossier de sortie
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def get_file_name(self, month):
        """Construit le nom du fichier parquet."""
        return f"{self.base_file_name}_{self.year}-{month:02d}.parquet"

    def get_file_path(self, month):
        """Retourne le chemin local du fichier parquet."""
        return self.data_dir / self.get_file_name(month)

    def file_exists(self, month):
        """Vérifie si le fichier existe localement."""
        return self.get_file_path(month).exists()

    def download_month(self, month):
        """Télécharge le fichier d’un mois donné."""
        file_path = self.get_file_path(month)

        if self.file_exists(month):
            print(f"⚠️ Le fichier {file_path} existe déjà")
            return file_path

        file_url = f"{self.base_url}/{self.get_file_name(month)}"
        print(f"⬇️  Téléchargement de {file_url} ...")

        try:
            response = requests.get(file_url, stream=True, timeout=30)
            response.raise_for_status()

            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"✅ Fichier téléchargé : {file_path}")
            return file_path

        except requests.RequestException as e:
            print(f"❌ Erreur lors du téléchargement de {file_url} : {e}")
            return None
    
    def download_all_available(self):
        """Télécharge tous les fichiers disponibles jusqu’au mois courant."""
        now = datetime.now()
        files_downloaded = []

        # Si on télécharge pour l'année courante → ne pas dépasser le mois actuel
        last_month = 12 if self.year < now.year else now.month

        for month in range(1, last_month + 1):
            file = self.download_month(month)
            if file:
                files_downloaded.append(file)

        print(f"✅ {len(files_downloaded)} fichiers téléchargés ou déjà présents.")
        return files_downloaded


# 🔽 Code exécuté seulement si le script est lancé directement
if __name__ == "__main__":
    downloader = NYCTaxiDataDownloader(
        base_url="https://d37ci6vzurychx.cloudfront.net/trip-data",
        year=datetime.now().year,
        data_dir="./data/raw",
        base_file_name="yellow_tripdata"
    )
    downloader.download_all_available()
