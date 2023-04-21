import os
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class Watcher:
    def __init__(self, path):
        self.path = path
        self.event_handler = Handler()
        self.observer = Observer()

    def run(self):
        self.observer.schedule(
            self.event_handler, self.path, recursive=True
        )
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()


class Handler(FileSystemEventHandler):
    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == "created":
            file_path = event.src_path
            file_name, file_extension = os.path.splitext(file_path)
            if file_extension in [".csv", ".xlsx", ".xlsm", ".xlsb", ".xls"]:
                df = pd.read_excel(file_path)
                df.to_csv("consolidado.csv", mode="a", header=False)
                print(f"{file_name} ha sido agregado al consolidado.")
            else:
                invalid_path = os.path.join(os.path.dirname(file_path), "archivos_invalidos")
                if not os.path.exists(invalid_path):
                    os.mkdir(invalid_path)
                os.rename(file_path, os.path.join(invalid_path, os.path.basename(file_path)))
                print(f"{file_name} no es un archivo válido y ha sido movido a la carpeta 'archivos_invalidos'.")


if __name__ == "__main__":
    path = input("Ingrese la ruta de la carpeta a monitorear: ")
    w = Watcher(path)
    w.run()
