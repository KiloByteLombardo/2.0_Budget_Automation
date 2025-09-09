#!/usr/bin/env python3
from __future__ import annotations
import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import traceback
from pipeline.runners import run_colombia_mercancia, run_venezuela_mercancia
from pipeline.export import write_excel_with_raw

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Presupuesto Mercancía (CO / VE) - Pandas + YAML")
        self.geometry("820x540"); self.resizable(False, False)

        self.var_country = tk.StringVar(value="Colombia")
        self.var_schema  = tk.StringVar(value="./schema/schema.yaml")
        self.var_cfg     = tk.StringVar(value="./schema/colombia.yaml")
        self.var_ebs     = tk.StringVar(value="")
        self.var_reim    = tk.StringVar(value="")
        self.var_rsf     = tk.StringVar(value="")
        self.var_out     = tk.StringVar(value="./mercancia.xlsx")
        self.var_exec    = tk.StringVar(value=pd.Timestamp.today().strftime("%Y-%m-%d"))

        row=0
        tk.Label(self, text="País:").grid(row=row, column=0, padx=10, pady=6, sticky="w")
        opt = tk.OptionMenu(self, self.var_country, "Colombia", "Venezuela", command=self.on_country_change)
        opt.config(width=20); opt.grid(row=row, column=1, padx=6, pady=6, sticky="w"); row+=1

        def add_row(lbl, var, cmd):
            nonlocal row
            tk.Label(self, text=lbl, anchor="w").grid(row=row, column=0, padx=10, pady=6, sticky="w")
            tk.Entry(self, textvariable=var, width=70).grid(row=row, column=1, padx=6, pady=6, sticky="w")
            tk.Button(self, text="Buscar…", command=cmd).grid(row=row, column=2, padx=6, pady=6)
            row+=1
        add_row("Schema.yaml:", self.var_schema, self.pick_schema)
        add_row("Config país (.yaml):", self.var_cfg, self.pick_cfg)
        add_row("EBS (xlsx/csv):", self.var_ebs, self.pick_ebs)
        add_row("REIM (xlsx/csv):", self.var_reim, self.pick_reim)
        add_row("RSF (xlsx/csv):", self.var_rsf, self.pick_rsf)
        add_row("Salida (.xlsx):", self.var_out, self.pick_out)

        tk.Label(self, text="Fecha de ejecución (yyyy-mm-dd):").grid(row=row, column=0, padx=10, pady=6, sticky="w")
        tk.Entry(self, textvariable=self.var_exec, width=20).grid(row=row, column=1, padx=6, pady=6, sticky="w")
        tk.Label(self, text="*Se ajustará al lunes de esa semana.").grid(row=row, column=2, padx=6, pady=6, sticky="w"); row+=1

        self.btn_run = tk.Button(self, text="Generar Consolidado", command=self.run_job, height=2)
        self.btn_run.grid(row=row, column=0, columnspan=3, padx=10, pady=12, sticky="we"); row+=1

        self.log = tk.Text(self, height=12)
        self.log.grid(row=row, column=0, columnspan=3, padx=10, pady=8, sticky="nsew")
        self.grid_columnconfigure(1, weight=1)

    # --- Browsers (multilínea, sin ; ni # noqa) ---
    def pick_schema(self):
        p = filedialog.askopenfilename(
            title="Selecciona schema.yaml",
            filetypes=[("YAML", "*.yml *.yaml"), ("Todos", "*.*")],
        )
        if p:
            self.var_schema.set(p)

    def pick_cfg(self):
        p = filedialog.askopenfilename(
            title="Selecciona configuración del país (.yaml)",
            filetypes=[("YAML", "*.yml *.yaml"), ("Todos", "*.*")],
        )
        if p:
            self.var_cfg.set(p)

    def pick_ebs(self):
        p = filedialog.askopenfilename(
            title="Selecciona EBS",
            filetypes=[("Excel/CSV", "*.xlsx *.xls *.csv"), ("Todos", "*.*")],
        )
        if p:
            self.var_ebs.set(p)

    def pick_reim(self):
        p = filedialog.askopenfilename(
            title="Selecciona REIM",
            filetypes=[("Excel/CSV", "*.xlsx *.xls *.csv"), ("Todos", "*.*")],
        )
        if p:
            self.var_reim.set(p)

    def pick_rsf(self):
        p = filedialog.askopenfilename(
            title="Selecciona RSF",
            filetypes=[("Excel/CSV", "*.xlsx *.xls *.csv"), ("Todos", "*.*")],
        )
        if p:
            self.var_rsf.set(p)

    def pick_out(self):
        p = filedialog.asksaveasfilename(
            title="Guardar Excel",
            defaultextension=".xlsx",
            initialfile="mercancia.xlsx",
            filetypes=[("Excel", "*.xlsx"), ("Todos", "*.*")],
        )
        if p:
            self.var_out.set(p)

    def logln(self, msg: str):
            self.log.insert("end", msg+"\n"); self.log.see("end"); self.update_idletasks()

    def on_country_change(self, value):
            if value == "Colombia":  self.var_cfg.set("./schema/colombia.yaml")
            elif value == "Venezuela": self.var_cfg.set("./schema/venezuela.yaml")

    def run_job(self):
            try:
                self.btn_run.config(state="disabled"); self.log.delete("1.0","end")
                country = self.var_country.get().strip()
                schema  = self.var_schema.get().strip()
                cfg     = self.var_cfg.get().strip()
                ebs     = self.var_ebs.get().strip()
                reim    = self.var_reim.get().strip()
                rsf     = self.var_rsf.get().strip()
                out     = self.var_out.get().strip()
                exec_s  = self.var_exec.get().strip()

                for label, path in [("Schema",schema),("Config país",cfg),("EBS",ebs),("REIM",reim),("RSF",rsf),("Salida",out)]:
                    if not path:
                        messagebox.showerror("Falta información", f"Selecciona: {label}")
                        self.btn_run.config(state="normal"); return

                exec_date = pd.to_datetime(exec_s, errors="coerce")
                if pd.isna(exec_date):
                    messagebox.showerror("Fecha inválida", "Usa formato yyyy-mm-dd")
                    self.btn_run.config(state="normal"); return

                exec_mon = exec_date - pd.to_timedelta(exec_date.weekday(), unit="D")
                self.logln(f"País: {country}")
                self.logln(f"Ejecución (lunes): {exec_mon.date()}")
                self.logln("Leyendo y consolidando…")

                if country.lower() == "venezuela":
                    res = run_venezuela_mercancia(schema, cfg, ebs, reim, rsf, exec_date=exec_mon)
                else:
                    res = run_colombia_mercancia(schema, cfg, ebs, reim, rsf, exec_date=exec_mon)

                if isinstance(res, tuple):
                    if len(res)==3: df, raws, export_cfg = res
                    elif len(res)==2: df, raws = res; export_cfg = {}
                    else: df = res[0]; raws = {}; export_cfg={}
                else:
                    df = res; raws={}; export_cfg={}

                self.logln(f"Filas consolidadas: {len(df):,}")
                self.logln("Exportando a Excel…")
                tipo_map = export_cfg.get("__tipo_map") if country.lower()=="venezuela" else None
                if country.lower()=="venezuela" and (tipo_map is None or getattr(tipo_map, "empty", True)):
                    self.logln("AVISO: mini maestro PROVEEDOR→TIPO no disponible; 'Grupo de Pago' usará solo reglas DIRECTO/PPV RMS.")

                write_excel_with_raw(out, df, export_cfg, raw_sources=raws, exec_mon=exec_mon, tipo_map=tipo_map)
                self.logln(f"Listo: {out}")
                messagebox.showinfo("Éxito", f"Exportado:\n{out}")
            except Exception:
                err = traceback.format_exc(limit=10)
                self.logln("ERROR:\n"+err)
                messagebox.showerror("Error", err)
            finally:
                self.btn_run.config(state="normal")

if __name__ == "__main__":
    App().mainloop()
