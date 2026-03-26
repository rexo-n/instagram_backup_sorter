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

## 📸 How It Works

### 1. Input your Instagram export
![Step 1](images/step1.png)

### 2. Run the script
![Step 2](images/step2.png)

### 3. Explore clean chats
![Step 3](images/step3.png)

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