> Built for handling messy Instagram exports at scale.
# Instagram HTML Sorter

Advanced Instagram export parser with chat merging, media deduplication, and an interactive offline chat viewer.

---

## ✨ Features

- 🔗 Multi-part chat reconstruction
- 🧠 Message + media deduplication (SHA-256)
- 📂 Organized output structure
- 🖼️ Offline chat viewer (HTML)
- ⚡ Fast & resumable processing

---

## 📸 How to Use

### 1. Run the script
![Step 1](images/step_1_run.png)

### 2. Open menu
![Step 2](images/step_2_menu.png)

### 3. Select input folder
![Step 3](images/step_3_select_input.png)

### 4. Select output folder
![Step 4](images/step_4_select_output.png)

### 5. Start processing
![Step 5](images/step_5_processing.png)

---

## 🖥️ Viewer Preview
![Viewer](images/viewer_preview.png)

---

## ⚙️ Usage

```bash
python instagram_html_sort.py --input <path> --output <path>
```

---

## 📦 Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 📁 Output Structure

```
Chats/
Media/
manifest.json
```

---

## 📝 License

Licensed under the MIT License.