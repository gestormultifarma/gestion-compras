// JavaScript placeholder for charts

document.addEventListener('DOMContentLoaded', function () {
    const labels = JSON.parse(document.getElementById('graficoDatos').dataset.labels);
    const data = JSON.parse(document.getElementById('graficoDatos').dataset.data);
  
    const ctx = document.getElementById('miGrafico');
    const chart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{
          label: 'Ventas',
          data: data,
          backgroundColor: 'rgba(54, 162, 235, 0.5)',
          borderColor: 'rgba(54, 162, 235, 1)',
          borderWidth: 1
        }]
      },
      options: {
        responsive: true,
        scales: {
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: 'Cantidad vendida'
            }
          },
          x: {
            title: {
              display: true,
              text: 'Productos'
            }
          }
        },
        plugins: {
          legend: {
            display: true,
            position: 'top'
          },
          title: {
            display: true,
            text: 'Gráfico de Ventas por Producto'
          }
        }
      }
    });
  });
  