install.packages(c("sparklyr", "readxl", "dplyr", "ggplot2", "shiny"))

library(sparklyr)
library(readxl)
library(dplyr)
library(ggplot2)
library(shiny)

# Kết nối Spark
sc <- spark_connect(master = "local")

# Đường dẫn file
file_path <- "C:/Users/FPTSHOP/Desktop/big data/Assignment-1_Data.xlsx"

# Kiểm tra file tồn tại trước khi đọc
to_load <- if (file.exists(file_path)) read_excel(file_path) else stop("File không tồn tại!")

# Chọn các cột cần thiết
to_load <- to_load %>% select(BillNo, Itemname, Quantity, Date, Price, CustomerID, Country)

# Chuyển dữ liệu lên Spark
sales_tbl <- copy_to(sc, to_load, "sales_data", overwrite = TRUE)

# Chuẩn hóa tên cột
sales_tbl <- sales_tbl %>% rename_with(tolower)

# Kiểm tra và xử lý dữ liệu
sales_tbl <- sales_tbl %>%
  filter(!is.na(date), !is.na(country), !is.na(quantity)) %>%
  mutate(date = as.Date(date))

# Chuyển dữ liệu từ Spark về R để vẽ biểu đồ
sales_data_r <- sales_tbl %>% collect()

# UI của Shiny
ui <- fluidPage(
  titlePanel("Phân tích hành vi bán hàng"),
  sidebarLayout(
    sidebarPanel(
      selectInput("country", "Chọn quốc gia", choices = unique(sales_data_r$country), selected = unique(sales_data_r$country)[1])
    ),
    mainPanel(
      plotOutput("salesTrend"),
      plotOutput("productSales"),
      plotOutput("customerTrends")
    )
  )
)

# Server của Shiny
server <- function(input, output, session) {
  filtered_data <- reactive({
    sales_data_r %>% filter(country == input$country)
  })
  
  # Doanh thu theo thời gian
  output$salesTrend <- renderPlot({
    ggplot(filtered_data(), aes(x = date, y = quantity * price)) +
      geom_line(color = "blue") +
      ggtitle(paste("Doanh thu theo thời gian -", input$country)) +
      xlab("Ngày") + ylab("Doanh thu")
  })
  
  # Số lượng sản phẩm bán ra theo quốc gia
  output$productSales <- renderPlot({
    filtered_data() %>%
      group_by(itemname) %>%
      summarise(total_sales = sum(quantity), .groups = 'drop') %>%
      ggplot(aes(x = reorder(itemname, -total_sales), y = total_sales, fill = itemname)) +
      geom_bar(stat = "identity") +
      coord_flip() +
      ggtitle(paste("Số lượng sản phẩm bán ra -", input$country)) +
      xlab("Sản phẩm") + ylab("Số lượng") +
      theme(legend.position = "none")
  })
  
  # Phân tích xu hướng khách hàng
  output$customerTrends <- renderPlot({
    filtered_data() %>%
      group_by(customerid) %>%
      summarise(total_spent = sum(quantity * price), purchase_count = n(), .groups = 'drop') %>%
      ggplot(aes(x = purchase_count, y = total_spent)) +
      geom_point(color = "red") +
      ggtitle(paste("Xu hướng khách hàng -", input$country)) +
      xlab("Số lần mua hàng") + ylab("Tổng chi tiêu")
  })
}

# Chạy ứng dụng với xử lý ngắt kết nối Spark
onStop(function() {
  spark_disconnect(sc)
})

shinyApp(ui = ui, server = server)